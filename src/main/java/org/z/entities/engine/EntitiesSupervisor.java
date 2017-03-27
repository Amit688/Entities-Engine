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
import org.apache.kafka.clients.producer.ProducerRecord;

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
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;


/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<EntitiesEvent> {
    private Materializer materializer;
    private KafkaSourceFactory sourceFactory;
    private SchemaRegistryClient schemaRegistry;
    private Map<UUID, StreamDescriptor> streams;
    
    public EntitiesSupervisor(Materializer materializer, KafkaSourceFactory sourceFactory, SchemaRegistryClient schemaRegistry) {
        this.materializer = materializer;
        this.sourceFactory = sourceFactory;
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
		SourceDescriptor sourceDescriptor = new SourceDescriptor(
				data.get("sourceName").toString(), // Is actually a org.apache.avro.util.Utf8
				data.get("externalSystemID").toString());
		System.out.println("creating entity manager stream for source " + sourceDescriptor);
		createStream(sourceDescriptor);
    }
    
    private void merge(GenericRecord data) {
		List<Utf8> idsToMerge = (List<Utf8>) data.get("mergedEntitiesId");
    	List<UUID> uuidsToMerge = toUUIDs(idsToMerge);
		System.out.println("got merge event for:");
		uuidsToMerge.forEach(System.out::println);
    	if (streams.keySet().containsAll(uuidsToMerge)) {
    		List<SourceDescriptor> sourceDescriptorsToMerge = killAndFlattenSources(uuidsToMerge);
    		Source<ConsumerRecord<String, Object>, NotUsed> mergedSource = 
    				Source.fromGraph(GraphDSL.create(builder -> createMergedSourceGraph(builder, sourceDescriptorsToMerge)));
    		createStream(mergedSource, sourceDescriptorsToMerge);
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
			stream.getKillSwitch().shutdown();
			sources.addAll(stream.getSourceDescriptors());
		}
		return sources;
	}
	
	private SourceShape<ConsumerRecord<String, Object>> createMergedSourceGraph(
			Builder<NotUsed> builder, List<SourceDescriptor> sourceDescriptors) {
		UniformFanInShape<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> merger = 
				builder.add(Merge.create(sourceDescriptors.size()));
		
		for (SourceDescriptor sourceDescriptor : sourceDescriptors) {
			Source<ConsumerRecord<String, Object>, Consumer.Control> source = sourceFactory.create(sourceDescriptor);
			Outlet<ConsumerRecord<String, Object>> outlet = builder.add(source).out();
			builder.from(outlet).toFanIn(merger);
		}
		return SourceShape.of(merger.out());
	}
    
    private void split(GenericRecord data) {
    	String idToSplit = data.get("splitedEntityID").toString();
    	UUID uuidToSplit = UUID.fromString(idToSplit);
		System.out.println("got split event for: " + uuidToSplit.toString());
    	StreamDescriptor splittedStream = streams.remove(uuidToSplit);
    	if (splittedStream == null) {
    		throw new RuntimeException("tried to split non existent entity with id " + idToSplit);
    	}
    	
    	splittedStream.getKillSwitch().shutdown();
    	for (SourceDescriptor sourceDescriptor : splittedStream.getSourceDescriptors()) {
    		createStream(sourceDescriptor);
    	}
    }
    
    private void createStream(SourceDescriptor sourceDescriptor) {
    	createStream(sourceFactory.create(sourceDescriptor), Arrays.asList(sourceDescriptor));
    }
    
    private void createStream(Source<ConsumerRecord<String, Object>, ?> source, 
			List<SourceDescriptor> sourceDescriptors) {
//		UUID uuid = UUID.randomUUID();
		UUID uuid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00" + sourceDescriptors.get(0).getSensorId().substring(sourceDescriptors.get(0).getSensorId().length() - 2));
    	EntityManager entityManager = new EntityManager(uuid, schemaRegistry);
    	UniqueKillSwitch killSwitch = source
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(Sink.foreach(EntitiesSupervisor::dummySink))
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streams.put(uuid, 
    			new StreamDescriptor(killSwitch, uuid, sourceDescriptors));
	}
    
    private static void dummySink(ProducerRecord<String, GenericRecord> record) {
    	System.out.println("wrote report to sink: " + record.value());
    }
}
