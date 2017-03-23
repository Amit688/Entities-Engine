package org.z.entities.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import akka.NotUsed;
import akka.actor.ActorSystem;
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


/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<EntitiesEvent> {
    ActorSystem system;
    Materializer materializer;
    Map<UUID, StreamDescriptor> streamDescriptors;
    
    public EntitiesSupervisor(ActorSystem system, Materializer materializer) {
    	this.system = system;
        this.materializer = materializer;
        streamDescriptors = new HashMap<>();
    }

    @Override
    public void accept(EntitiesEvent event) {
    	try {
	    	switch (event.type) {
			case CREATE:
				create(event.data);
				break;
			case MERGE:
				merge(event.data);
				break;
			case SPLIT:
				split(event.data);
				break;
			default:
				System.out.println("received unknown event type: " + event.type);
				break;
	    	}
    	} catch (RuntimeException e) {
    		System.out.println("failed to process event of type " + event.type);
    		System.out.println("event data: " + event.data);
    		e.printStackTrace();
    	}
    }
    
    public void create(GenericRecord data) {
		TopicDescriptor topicDescriptor = new TopicDescriptor(
				(String) data.get("sourceName"), 
				(String) data.get("externalSystemID"));
		System.out.println("creating entity manager stream for topic " + topicDescriptor);
		createStream(topicDescriptor);
    }
    
    private void merge(GenericRecord data) {
    	@SuppressWarnings("unchecked")
		List<String> idsToMerge = (List<String>) data.get("mergedEntitiesId");
    	List<UUID> uuidsToMerge = toUUIDs(idsToMerge);
    	
    	if (streamDescriptors.keySet().containsAll(uuidsToMerge)) {
    		List<TopicDescriptor> topicDescriptorsToMerge = new ArrayList<>(uuidsToMerge.size());
    		for (UUID uuid : uuidsToMerge) {
    			StreamDescriptor stream = streamDescriptors.remove(uuid);
    			stream.getKillSwitch().shutdown();
    			topicDescriptorsToMerge.addAll(stream.getTopicDescriptors());
    		}
    		Source<ConsumerRecord<String, Object>, NotUsed> mergedSource = 
    				Source.fromGraph(GraphDSL.create(builder -> createMergedSourceGraph(builder, topicDescriptorsToMerge)));
    		createStream(mergedSource, topicDescriptorsToMerge);
    	} else {
    		uuidsToMerge.removeAll(streamDescriptors.keySet());
    		String debugString = uuidsToMerge.stream().map(UUID::toString).collect(Collectors.joining(", "));
    		throw new RuntimeException("tried to merge non existent entities: " + debugString);
    	}
    }

	private List<UUID> toUUIDs(List<String> ids) {
    	List<UUID> uuids = new ArrayList<>(ids.size());
    	for (String id : ids) {
    		uuids.add(UUID.fromString(id));
    	}
    	return uuids;
    }
	
	private SourceShape<ConsumerRecord<String, Object>> createMergedSourceGraph(
			Builder<NotUsed> builder, List<TopicDescriptor> topicDescriptorsToMerge) {
		UniformFanInShape<ConsumerRecord<String, Object>, ConsumerRecord<String, Object>> merger = 
				builder.add(Merge.create(topicDescriptorsToMerge.size()));
		for (TopicDescriptor topic : topicDescriptorsToMerge) {
			Source<ConsumerRecord<String, Object>, ?> source = KafkaSourceFactory.create(system, topic);
			Outlet<ConsumerRecord<String, Object>> outlet = builder.add(source).out();
			builder.from(outlet).toFanIn(merger);
		}
		return SourceShape.of(merger.out());
	}
    
    private void split(GenericRecord data) {
    	String idToSplit = (String) data.get("splitedEntityID");
    	UUID uuidToSplit = UUID.fromString(idToSplit);
    	StreamDescriptor splittedDescriptor = streamDescriptors.remove(uuidToSplit);
    	if (splittedDescriptor == null) {
    		throw new RuntimeException("tried to split non existent entity with id " + idToSplit);
    	}
    	
    	splittedDescriptor.getKillSwitch().shutdown();
    	for (TopicDescriptor topicDescriptor : splittedDescriptor.getTopicDescriptors()) {
    		createStream(topicDescriptor);
    	}
    }
    
    private void createStream(TopicDescriptor topicDescriptor) {
    	createStream(KafkaSourceFactory.create(system, topicDescriptor), Arrays.asList(topicDescriptor));
    }
    
    private void createStream(Source<ConsumerRecord<String, Object>, ?> source, 
			List<TopicDescriptor> topicDescriptors) {
		UUID uuid = UUID.randomUUID();
    	EntityManager entityManager = new EntityManager(uuid);
    	UniqueKillSwitch killSwitch = source
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(Sink.foreach(EntitiesSupervisor::dummySink))
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streamDescriptors.put(uuid, 
    			new StreamDescriptor(killSwitch, uuid, topicDescriptors));
	}
    
    private static void dummySink(ProducerRecord<String, GenericRecord> record) {
    	System.out.println("wrote report to sink: " + record.value());
    }
}
