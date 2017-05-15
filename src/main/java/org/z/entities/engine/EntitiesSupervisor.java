package org.z.entities.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
    private Map<UUID, GenericRecord> entities;
    
    public EntitiesSupervisor(Materializer materializer, KafkaComponentsFactory componentsFactory) {
        this.materializer = materializer;
        this.componentsFactory = componentsFactory;
        streams = new ConcurrentHashMap<>();
        this.entities = new HashMap<>();
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
    	System.out.println("DATA IS: \n" + data.toString());
		SourceDescriptor sourceDescriptor = new SourceDescriptor(
				data.get("sourceName").toString(), // Is actually a org.apache.avro.util.Utf8
				data.get("externalSystemID").toString(),
				UUID.randomUUID());
		System.out.println("creating entity manager stream for source " + sourceDescriptor);
		createStream(sourceDescriptor, sourceDescriptor.getSystemUUID(), "NONE");
    }
    
    private void merge(GenericRecord data) {
		@SuppressWarnings("unchecked")  // From schema
		List<Utf8> idsToMerge = (List<Utf8>) data.get("mergedEntitiesId");
    	List<UUID> uuidsToMerge = toUUIDs(idsToMerge);
		System.out.println("got merge event for:");
		uuidsToMerge.forEach(System.out::println);
    	if (streams.keySet().containsAll(uuidsToMerge)) {
    		List<SourceDescriptor> sonsSources = killAndFlattenSonsAttributes(uuidsToMerge);
    		Source<ConsumerRecord<Object, Object>, NotUsed> mergedSource =
    				Source.fromGraph(GraphDSL.create(builder -> createMergedSourceGraph(builder, sonsSources)));
    		createStream(mergedSource, sonsSources, UUID.randomUUID(), "MERGED");
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
	
	private List<SourceDescriptor> killAndFlattenSonsAttributes(List<UUID> uuidsToKill) {
		List<SourceDescriptor> sources = new ArrayList<>(streams.size());
		for (UUID uuid : uuidsToKill) {
			StreamDescriptor stream = streams.remove(uuid);
			stopStream(stream);
			sources.addAll(stream.getSourceDescriptors());
		}
		return sources;
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
	
	private SourceShape<ConsumerRecord<Object, Object>> createMergedSourceGraph(
			Builder<NotUsed> builder,List<SourceDescriptor> sonsSources) {
		UniformFanInShape<ConsumerRecord<Object, Object>, ConsumerRecord<Object, Object>> merger =
				builder.add(Merge.create(sonsSources.size()));
		
		for (SourceDescriptor entry : sonsSources) {
//			Long offset = (Long) ((GenericRecord) entry.getValue().get("basicAttributes")).get("entityOffset");
			Source<ConsumerRecord<Object, Object>, Consumer.Control> source = componentsFactory.getSource(entry);
			Outlet<ConsumerRecord<Object, Object>> outlet = builder.add(source).out();
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
    	createStream(componentsFactory.getSource(sourceDescriptor), Arrays.asList(sourceDescriptor), uuid,
				stateChange);
    }
    
    private void createStream(Source<ConsumerRecord<Object, Object>, ?> source,
			List<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange) {
    	EntityManager entityManager = new EntityManager(uuid, stateChange, sourceDescriptors, entities);
    	UniqueKillSwitch killSwitch = source
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(componentsFactory.getSink())
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streams.put(uuid, 
    			new StreamDescriptor(killSwitch, uuid, sourceDescriptors));
	}
    
//    private void createStream(Source<ConsumerRecord<String, Object>, ?> source,
//			Map<SourceDescriptor, GenericRecord> sonsAttributes, UUID uuid, String stateChange) {
//    	EntityManager entityManager = new EntityManager(uuid, stateChange, sonsAttributes, schemaRegistry, entities);
//    	UniqueKillSwitch killSwitch = source
//    			.viaMat(KillSwitches.single(), Keep.right())
//    			.via(Flow.fromFunction(entityManager::apply))
//    			.to(componentsFactory.getSink())
//    			.run(materializer);
//
//    	System.out.println("storing stream descriptor for later use");
//    	streams.put(uuid,
//    			new StreamDescriptor(killSwitch, uuid, new ArrayList<>(sonsAttributes.keySet())));
//	}

    private void stopStream(StreamDescriptor stream) {
    	stream.getKillSwitch().shutdown();
    }
}
