package org.z.entities.engine;

import java.net.URL;
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


/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<EntitiesEvent> {
    private Materializer materializer;
    private KafkaComponentsFactory componentsFactory;
    private Map<UUID, StreamDescriptor> streams;
    
    public EntitiesSupervisor(Materializer materializer, KafkaComponentsFactory componentsFactory) {
        this.materializer = materializer;
        this.componentsFactory = componentsFactory;
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
				((GenericRecord) data.get("basicAttributes")).get("sourceName").toString(), // Is actually a org.apache.avro.util.Utf8
				data.get("externalSystemID").toString(),
				UUID.randomUUID());
		System.out.println("creating entity manager stream for source " + sourceDescriptor);
		createStream(sourceDescriptor, sourceDescriptor.getSystemUUID(), "NONE");
    }
    
    private void merge(GenericRecord data) {
		@SuppressWarnings("unchecked")  // From schema
		List<GenericRecord> familiesToMerge = (List<GenericRecord>) data.get("mergedFamilies");
		List<UUID> uuidsToMerge = getUUIDs(familiesToMerge);
		System.out.println("got merge event for:");
		uuidsToMerge.forEach(System.out::println);
    	if (streams.keySet().containsAll(uuidsToMerge)) {
    		Map<SourceDescriptor, GenericRecord> sonsAttributesToMerge = killAndFlattenSonsAttributes(familiesToMerge);
    		Source<ConsumerRecord<String, Object>, NotUsed> mergedSource = 
    				Source.fromGraph(GraphDSL.create(builder -> createMergedSourceGraph(builder, sonsAttributesToMerge)));
    		createStream(mergedSource, sonsAttributesToMerge, UUID.randomUUID(), "MERGED");
    	} else {
    		uuidsToMerge.removeAll(streams.keySet());
    		String debugString = uuidsToMerge.stream().map(UUID::toString).collect(Collectors.joining(", "));
    		throw new RuntimeException("tried to merge non existent entities: " + debugString);
    	}
    }
    
    private List<UUID> getUUIDs(List<GenericRecord> families) {
    	List<UUID> uuids = new ArrayList<>(families.size());
    	for (GenericRecord family : families) {
    		uuids.add(getUUID(family));
    	}
    	return uuids;
    }
	
	private Map<SourceDescriptor, GenericRecord> killAndFlattenSonsAttributes(List<GenericRecord> familiesToKill) {
		Map<SourceDescriptor, GenericRecord> sons = new HashMap<>(familiesToKill.size());
		for (GenericRecord family : familiesToKill) {
			UUID uuid = getUUID(family);
			StreamDescriptor stream = streams.remove(uuid);
			stopStream(stream);
			collectSons(family, sons);
		}
		return sons;
	}
	
	private UUID getUUID(GenericRecord family) {
		return UUID.fromString(family.get("entityID").toString());
	}
	
	private void collectSons(GenericRecord family, Map<SourceDescriptor, GenericRecord> collector) {
		@SuppressWarnings("unchecked")
		List<GenericRecord> familySons = (List<GenericRecord>) family.get("sons");
		for (GenericRecord son : familySons) {
			SourceDescriptor source = getSourceDescriptor(son);
			GenericRecord sonAttributes = (GenericRecord) son.get("generalEntityAttributes");
			collector.put(source, sonAttributes);
		}
	}
	
	private SourceDescriptor getSourceDescriptor(GenericRecord son) {
		UUID uuid = UUID.fromString(son.get("entityID").toString());
		GenericRecord attributes = (GenericRecord) son.get("entityAttributes");
		String externalSystemID = attributes.get("externalSystemID").toString();
		String sourceName = ((GenericRecord) attributes.get("basicAttributes")).get("sourceName").toString();
		return new SourceDescriptor(sourceName, externalSystemID, uuid);
	}
	
	private SourceShape<ConsumerRecord<String, Object>> createMergedSourceGraph(
			Builder<NotUsed> builder, Map<SourceDescriptor, GenericRecord> sonsAttributes) {
		UniformFanInShape<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> merger = 
				builder.add(Merge.create(sonsAttributes.size()));
		
		for (Map.Entry<SourceDescriptor, GenericRecord> entry : sonsAttributes.entrySet()) {
			Long offset = (Long) ((GenericRecord) entry.getValue().get("basicAttributes")).get("entityOffset");
			Source<ConsumerRecord<String, Object>, Consumer.Control> source = componentsFactory.createSource(entry.getKey(), offset);
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
				createStream(sourceDescriptor, sourceDescriptor.getSystemUUID(), "WAS_SPLIT");
			}
    	}
    }
    
    private void createStream(SourceDescriptor sourceDescriptor, UUID uuid, String stateChange) {
    	createStream(componentsFactory.createSource(sourceDescriptor), Arrays.asList(sourceDescriptor), uuid,
				stateChange);
    }
    
    private void createStream(Source<ConsumerRecord<String, Object>, ?> source, 
			List<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange) {
    	EntityManager entityManager = new EntityManager(uuid, stateChange, sourceDescriptors);
    	UniqueKillSwitch killSwitch = source
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(componentsFactory.createSink())
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streams.put(uuid, 
    			new StreamDescriptor(killSwitch, uuid, sourceDescriptors));
	}
    
    private void createStream(Source<ConsumerRecord<String, Object>, ?> source, 
			Map<SourceDescriptor, GenericRecord> sonsAttributes, UUID uuid, String stateChange) {
    	EntityManager entityManager = new EntityManager(uuid, stateChange, sonsAttributes);
    	UniqueKillSwitch killSwitch = source
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(componentsFactory.createSink())
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streams.put(uuid, 
    			new StreamDescriptor(killSwitch, uuid, new ArrayList<>(sonsAttributes.keySet())));
	}

    private void stopStream(StreamDescriptor stream) {
    	stream.getKillSwitch().shutdown();
    }
}
