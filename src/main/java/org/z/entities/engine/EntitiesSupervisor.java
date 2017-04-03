package org.z.entities.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import akka.NotUsed;
import akka.kafka.javadsl.Consumer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;


/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<EntitiesEvent> {
    private Materializer materializer;
    private KafkaComponentsFactory componentsFactory;
    private SchemaRegistryClient schemaRegistry;
    private Map<UUID, StreamDescriptor> streams;
    
    public EntitiesSupervisor(Materializer materializer, KafkaComponentsFactory componentsFactory, SchemaRegistryClient schemaRegistry) {
        this.materializer = materializer;
        this.componentsFactory = componentsFactory;
        this.schemaRegistry = schemaRegistry;
        streams = new HashMap<>();
    }

    @Override
    public void accept(EntitiesEvent event) {
    	try {
    		GenericRecord data = event.getData();
	    	switch (event.getType()) {
			case CREATE:
				create(data);
				break;
			case MERGE:
				merge(data);
				break;
			case SPLIT:
				split(data);
				break;
			default:
				System.out.println("received unknown event type: " + Objects.toString(event.getType()));
				break;
	    	}
    	} catch (RuntimeException e) {
    		System.out.println("failed to process event of type " + Objects.toString(event.getType()));
    		e.printStackTrace();
    	}
    }
    
    public void create(GenericRecord data) {
    	UUID uuid = UUID.randomUUID();
		SourceDescriptor sourceDescriptor = new SourceDescriptor(
				data.get("sourceName").toString(), // Is actually a org.apache.avro.util.Utf8
				data.get("externalSystemID").toString(),
				uuid.toString());
		System.out.println("creating entity manager stream for source " + sourceDescriptor);
		createStream(sourceDescriptor, uuid, "NONE");
    }
    
    private void merge(GenericRecord data) {
		@SuppressWarnings("unchecked")  // From schema
		List<Utf8> idsToMerge = (List<Utf8>) data.get("mergedEntitiesId");
    	List<UUID> uuidsToMerge = toUUIDs(idsToMerge);
		System.out.println("got merge event for:");
		uuidsToMerge.forEach(System.out::println);
    	if (streams.keySet().containsAll(uuidsToMerge)) {
    		List<SourceDescriptor> sourceDescriptorsToMerge = killAndFlattenSources(uuidsToMerge);
    		Source<ConsumerRecord<String, Object>, NotUsed> mergedSource = 
    				Source.fromGraph(GraphDSL.create(builder -> createMergedSourceGraph(builder, sourceDescriptorsToMerge)));
    		createStream(mergedSource, sourceDescriptorsToMerge, uuidsToMerge.get(0), "MERGED");
    	} else {
    		uuidsToMerge.removeAll(streams.keySet());
    		String debugString = uuidsToMerge.stream().map(UUID::toString).collect(Collectors.joining(", "));
    		throw new RuntimeException("tried to merge non existent entities: " + debugString);
    	}
    }

	private List<UUID> toUUIDs(List<Utf8> ids) {
    	List<UUID> uuids = new ArrayList<>(ids.size());
    	for (Utf8 id : ids) {
    		uuids.add(UUID.fromString(id.toString()));
    	}
    	return uuids;
    }
	
	private List<SourceDescriptor> killAndFlattenSources(List<UUID> uuidsToKill) {
		List<SourceDescriptor> sources = new ArrayList<>(streams.size());
		for (UUID uuid : uuidsToKill) {
			StreamDescriptor stream = streams.remove(uuid);
			stopStream(stream);
			sources.addAll(stream.getSourceDescriptors());
		}
		return sources;
	}
	
	private SourceShape<ConsumerRecord<String, Object>> createMergedSourceGraph(
			Builder<NotUsed> builder, List<SourceDescriptor> sourceDescriptors) {
		UniformFanInShape<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> merger = 
				builder.add(Merge.create(sourceDescriptors.size()));
		
		for (SourceDescriptor sourceDescriptor : sourceDescriptors) {
			Source<ConsumerRecord<String, Object>, Consumer.Control> source = componentsFactory.createSource(sourceDescriptor);
			Outlet<ConsumerRecord<String, Object>> outlet = builder.add(source).out();
			builder.from(outlet).toFanIn(merger);
		}
		return SourceShape.of(merger.out());
	}
    
    private void split(GenericRecord data) {
    	String idToSplit = data.get("splittedEntityID").toString();
    	UUID uuidToSplit = UUID.fromString(idToSplit);
		System.out.println("got split event for: " + uuidToSplit.toString());
    	StreamDescriptor splittedStream = streams.remove(uuidToSplit);
    	if (splittedStream == null) {
    		throw new RuntimeException("tried to split non existent entity with id " + idToSplit);
    	}
    	
    	stopStream(splittedStream);
		Boolean first = true;
    	for (SourceDescriptor sourceDescriptor : splittedStream.getSourceDescriptors()) {
    		if (first) {
				createStream(sourceDescriptor, uuidToSplit, "SON_TAKEN");
				first = false;
			} else {
				UUID uuid = UUID.randomUUID();
				createStream(sourceDescriptor, uuid, "WAS_SPLIT");
			}
    	}
    }
    
    private void createStream(SourceDescriptor sourceDescriptor, UUID uuid, String stateChange) {
    	createStream(componentsFactory.createSource(sourceDescriptor), Arrays.asList(sourceDescriptor), uuid,
				stateChange);
    }
    
    private void createStream(Source<ConsumerRecord<String, Object>, ?> source, 
			List<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange) {
    	EntityManager entityManager = new EntityManager(uuid, schemaRegistry, stateChange);
    	UniqueKillSwitch killSwitch = source
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(componentsFactory.createSink())
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streams.put(uuid, 
    			new StreamDescriptor(killSwitch, uuid, sourceDescriptors));
	}

    private void stopStream(StreamDescriptor stream) {
    	stream.getKillSwitch().shutdown();
    }
}
