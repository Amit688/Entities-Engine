package org.z.entities.engine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;

import akka.actor.ActorSystem;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;

/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<EntitiesEvent> {
    ActorSystem system;
    Materializer materializer;
    Map<TopicDescriptor, StreamDescriptor> streamDescriptors;
    
    public EntitiesSupervisor(ActorSystem system, Materializer materializer) {
    	this.system = system;
        this.materializer = materializer;
        streamDescriptors = new HashMap<>();
    }

    @Override
    public void accept(EntitiesEvent event) {
    	
    	switch (event.type) {
		case CREATE:
			create(event.data);
			break;
		default:
			System.out.println("received unknown event type: " + event.type);
			break;
    	}
//    	System.out.println("accepting message: " + message);
//    	String[] parts = message.split(",");
//    	String action = parts[0];
//    	TopicDescriptor topicDescriptor = createTopicDescriptor(parts[1]);
//    	
//    	switch (action) {
//		case "create":
//			create(topicDescriptor);
//			break;
//		case "kill":
//			kill(topicDescriptor);
//			break;
//		case "merge":
//			List<TopicDescriptor> topicDescriptors = new ArrayList<>(parts.length);
//			for (int i=1; i<parts.length; i++) {
//				topicDescriptors.add(createTopicDescriptor(parts[i]));
//			}
//			merge(topicDescriptors);
//			break;
//		default:
//			System.out.println("received unknown action " + action);
//			break;
//		}
    }

//    public void kill(TopicDescriptor topicDescriptor) {
//    	System.out.println("killing stream for topic: " + topicDescriptor);
//    	StreamDescriptor streamDescriptor = streamDescriptors.remove(topicDescriptor);
//    	streamDescriptor.getKillSwitch().shutdown();
//    	for (TopicDescriptor descriptor : streamDescriptor.getTopicDescriptors()) {
//    		streamDescriptors.remove(descriptor);
//    	}
//        System.out.println("remaining stream amount: " + streamDescriptors.size());
//    }
    
    public void create(GenericRecord data) {
    	TopicDescriptor topicDescriptor = createTopicDescriptor(data);
    	System.out.println("creating entity manager stream for topic " + topicDescriptor);
    	UUID uuid = UUID.randomUUID();
    	EntityManager entityManager = new EntityManager(uuid);
    	UniqueKillSwitch killSwitch = KafkaSourceFactory.create(system, topicDescriptor)
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(Sink.foreach(EntitiesSupervisor::dummySink))
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streamDescriptors.put(topicDescriptor, 
    			new StreamDescriptor(killSwitch, uuid, Arrays.asList(topicDescriptor)));
    }
    
    private TopicDescriptor createTopicDescriptor(GenericRecord data) {
    	try {
    		return new TopicDescriptor(
    				(String) data.get("sourceName"), 
    				(String) data.get("externalSystemID"));
    	} catch (RuntimeException e) {
    		throw e;
    	}
    }
    
//    private void merge(List<TopicDescriptor> topicDescriptors) {
//    	System.out.println("Killing previous streams");
//    	for (TopicDescriptor descriptor : topicDescriptors) {
//    		if (streamDescriptors.containsKey(descriptor)) {
//    			kill(descriptor);
//    		}
//    	}
//    	
//    	System.out.println("creating merged stream");
//    	Graph<SourceShape<ConsumerRecord<String, String>>, ?> source = GraphDSL.create(builder -> {
//    		UniformFanInShape<ConsumerRecord<String, String>, ConsumerRecord<String, String>> merger = 
//    				builder.add(Merge.create(topicDescriptors.size()));
//    		for (TopicDescriptor descriptor : topicDescriptors) {
//    			builder.from(builder.add(createSource(descriptor)).out()).toFanIn(merger);
//    		}
//    		
//    		return SourceShape.of(merger.out());
//    	});
//    	UUID uuid = UUID.randomUUID();
//    	EntityManager entityManager = new EntityManager(uuid);
//    	UniqueKillSwitch killSwitch = Source.fromGraph(source)
//    			.viaMat(KillSwitches.single(), Keep.right())
//        		.via(Flow.fromFunction(EntitiesSupervisor::convertConsumerRecord))
//        		.via(Flow.fromFunction(entityManager::apply))
//        		.to(Sink.foreach(EntitiesSupervisor::dummySink))
//        		.run(materializer);
//    	
//    	System.out.println("storing stream descriptor for later use");
//    	StreamDescriptor streamDescriptor = new StreamDescriptor(killSwitch, uuid, topicDescriptors);
//    	for (TopicDescriptor descriptor : topicDescriptors) {
//    		streamDescriptors.put(descriptor, streamDescriptor);
//    	}
//    	
//    }
    
    private static void dummySink(GenericRecord record) {
    	System.out.println("wrote report to sink: " + record);
    }

}
