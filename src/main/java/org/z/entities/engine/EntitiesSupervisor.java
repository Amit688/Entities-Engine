package org.z.entities.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import akka.NotUsed;
import akka.japi.function.Procedure;
import akka.kafka.javadsl.Consumer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.OverflowStrategy;
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
import akka.stream.javadsl.SourceQueue;
import akka.stream.javadsl.SourceQueueWithComplete;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.eventhandling.EventBus;


/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<EntitiesEvent> {
    private Materializer materializer;
    private KafkaComponentsFactory componentsFactory;
    private Map<UUID, StreamDescriptor> streams;
    private Map<UUID, GenericRecord> entities;
    private Map<String, BackOffice> backOfficeMap;
    private EventBus eventBus;
	private Map<UUID, EntityManagerForMailRoom> allEntityManager;
    
    public EntitiesSupervisor(Materializer materializer, KafkaComponentsFactory componentsFactory, Map<String, BackOffice> backOfficeMap) {
        this.materializer = materializer;
        this.componentsFactory = componentsFactory;
        this.streams = new ConcurrentHashMap<>();
        this.entities = new HashMap<>();
        this.backOfficeMap = backOfficeMap;
        this.eventBus = null; // axon needs to be initialized before we can set this
		this.allEntityManager = new ConcurrentHashMap<>();
    }

	public Map<UUID, EntityManagerForMailRoom> geAllEntityManager() {
		return allEntityManager;
	}

    public void setEventBus(EventBus eventBus) {
    	this.eventBus = eventBus;
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
    
    public Map<UUID, StreamDescriptor> getStreams() {
		return streams;
	}
    
    private void create(GenericRecord data) {
    	System.out.println("DATA IS: \n" + data.toString());
		SourceDescriptor sourceDescriptor = new SourceDescriptor(
				data.get("sourceName").toString(), // Is actually a org.apache.avro.util.Utf8
				data.get("externalSystemID").toString(),
				(long)data.get("dataOffset"),
				UUID.randomUUID());
		System.out.println("creating entity manager stream for source " + sourceDescriptor);
		createStream(sourceDescriptor, sourceDescriptor.getSystemUUID(), "NONE",data.get("sourceName").toString());
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
    		createStream(mergedSource, sonsSources, UUID.randomUUID(), "MERGED",data.get("sourceName").toString());
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
			stopStream(stream, null);
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
			SourceDescriptor source = SonAccessor.getSourceDescriptor(son);
			GenericRecord sonAttributes = SonAccessor.getAttributes(son);
			collector.put(source, sonAttributes);
		}
	}

//	private SourceDescriptor getSourceDescriptor(GenericRecord son) {
//		UUID uuid = UUID.fromString(son.get("entityID").toString());
//		GenericRecord attributes = (GenericRecord) son.get("entityAttributes");
//		String externalSystemID = attributes.get("externalSystemID").toString();
//		String sourceName = ((GenericRecord) attributes.get("basicAttributes")).get("sourceName").toString();
//		return new SourceDescriptor(sourceName, externalSystemID,0, uuid);
//	}
	
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
    	
    	stopStream(splittedStream, null);
		Boolean first = true;
    	for (SourceDescriptor sourceDescriptor : splittedStream.getSourceDescriptors()) {
    		if (first) {
				createStream(sourceDescriptor, uuidToSplit, "SON_TAKEN",data.get("sourceName").toString());
				first = false;
			} else {
				createStream(sourceDescriptor, sourceDescriptor.getSystemUUID(), "WAS_SPLIT",data.get("sourceName").toString());
			}
    	}
    }
    
    private void createStream(SourceDescriptor sourceDescriptor, UUID uuid, String stateChange,String sourceName) {
    	createStream(componentsFactory.getSource(sourceDescriptor), Arrays.asList(sourceDescriptor), uuid,
				stateChange,sourceName);
    }

    public void createStream(Source<ConsumerRecord<Object, Object>, ?> source,
    		List<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange,String sourceName) {
    	//EntityManager entityManager = new EntityManager(uuid, stateChange, sourceDescriptors, entities);
    	EntityManagerForMailRoom entityManagerForMailRoom = new EntityManagerForMailRoom(uuid, stateChange,
				sourceDescriptors, entities, eventBus);

     /*
    	UniqueKillSwitch killSwitch = source

    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(componentsFactory.getSink())
    			.run(materializer);
    	*/
     for (SourceDescriptor sourceDescriptor : sourceDescriptors) {
		 SourceQueue<GenericRecord> sourceQueue =
				 Source.<GenericRecord>queue(Integer.MAX_VALUE, OverflowStrategy.backpressure())
						 .via(Flow.fromFunction(entityManagerForMailRoom::apply))
						 .to(componentsFactory.getSink())
						 .run(materializer);

		 sourceDescriptor.setSourceQueue(sourceQueue);

		 backOfficeMap.get(sourceDescriptor.getSensorId()).updateTheSourceQueue(sourceDescriptor.getReportsId(), sourceQueue);
	 }
		allEntityManager.put(uuid, entityManagerForMailRoom);



    //	System.out.println("storing stream descriptor for later use");
    	// streams.put(uuid,
    	//		new StreamDescriptor(killSwitch, uuid, sourceDescriptors));


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

	public void createStream(Source<ConsumerRecord<Object, Object>, ?> source,
							 List<SourceDescriptor> sourceDescriptors, SourceDescriptor preferredSource, UUID uuid, String stateChange,String sourceName) {
		//EntityManager entityManager = new EntityManager(uuid, stateChange, sourceDescriptors, entities);
		EntityManagerForMailRoom entityManagerForMailRoom = new EntityManagerForMailRoom(uuid, stateChange,
				sourceDescriptors, preferredSource, entities, eventBus);

     /*
    	UniqueKillSwitch killSwitch = source

    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(componentsFactory.getSink())
    			.run(materializer);
    	*/
		for (SourceDescriptor sourceDescriptor : sourceDescriptors) {
			SourceQueue<GenericRecord> sourceQueue =
					Source.<GenericRecord>queue(Integer.MAX_VALUE, OverflowStrategy.backpressure())
							.via(Flow.fromFunction(entityManagerForMailRoom::apply))
							.to(componentsFactory.getSink())
							.run(materializer);

			sourceDescriptor.setSourceQueue(sourceQueue);

			backOfficeMap.get(sourceDescriptor.getSensorId()).updateTheSourceQueue(sourceDescriptor.getReportsId(), sourceQueue);
		}
		allEntityManager.put(uuid, entityManagerForMailRoom);
	}

    public void pauseEntity(UUID id, UUID sagaId) {
    	EntityManagerForMailRoom entityManager = allEntityManager.get(id);
        if (entityManager != null) {
			Collection<SourceDescriptor> sources = entityManager.getSonsSources();
            for (SourceDescriptor source : sources) {
				stopStream(source.getSensorId(), source.getReportsId(), sagaId);
			}
			allEntityManager.remove(id);
        } else {
            //TODO- handle trying to pause entity that doesn't exist
        }
    }

	private void stopStream(StreamDescriptor stream, UUID sagaId) {
		// TODO- find better way to stop a stream (without stopping each source separately)
		for (SourceDescriptor sourceDescriptor : stream.getSourceDescriptors()) {
			stopStream(sourceDescriptor.getSensorId(), sourceDescriptor.getReportsId(), sagaId);
		}
	}

	private void stopStream(String sourceName, String externalSystemId, UUID sagaId) {
    	System.out.println("stopping source queue stream");
		backOfficeMap.get(sourceName).stopSourceQueueStream(externalSystemId, sagaId);
	}

    private Predicate<ProducerRecord<Object, Object>> getPredicateForQueue() {

        Predicate<ProducerRecord<Object, Object>> predicate = new Predicate<ProducerRecord<Object, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(ProducerRecord<Object, Object> record) {

                String extrenalSystemId = ((GenericRecord)record.value()).get("entityID").toString();
                System.out.println("\n\nThe Predicate got extrenalSystemId "+extrenalSystemId);

                if (((GenericRecord)record.value()).get("stateChanges").toString().equals("STOPME")) {

                    System.out.println("The Predicate got STOP ME fot UUID "+extrenalSystemId);
                    return false;
                }
                else {
                    return true;
                }
            }
        };

        return predicate;

    }
}
