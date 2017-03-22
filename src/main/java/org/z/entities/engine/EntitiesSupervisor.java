package org.z.entities.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.z.entities.engine.data.Coordinate;
import org.z.entities.engine.data.EntityReport;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Graph;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.SourceShape;
import akka.stream.ThrottleMode;
import akka.stream.UniformFanInShape;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.concurrent.duration.FiniteDuration;

/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<String> {
    ActorSystem system;
    Materializer materializer;
    Map<TopicDescriptor, StreamDescriptor> streamDescriptors;
    
    public EntitiesSupervisor(ActorSystem system, Materializer materializer) {
    	this.system = system;
        this.materializer = materializer;
        streamDescriptors = new HashMap<>();
    }

    @Override
    public void accept(String message) {
    	System.out.println("accepting message: " + message);
    	String[] parts = message.split(",");
    	String action = parts[0];
    	TopicDescriptor topicDescriptor = createTopicDescriptor(parts[1]);
    	
    	switch (action) {
		case "create":
			create(topicDescriptor);
			break;
		case "kill":
			kill(topicDescriptor);
			break;
		case "merge":
			List<TopicDescriptor> topicDescriptors = new ArrayList<>(parts.length);
			for (int i=1; i<parts.length; i++) {
				topicDescriptors.add(createTopicDescriptor(parts[i]));
			}
			merge(topicDescriptors);
			break;
		default:
			System.out.println("received unknown action " + action);
			break;
		}
    }
    
    private TopicDescriptor createTopicDescriptor(String topic) {
    	String[] parts = topic.split(":");
    	return new TopicDescriptor(parts[0], parts[1]);
    }
    
    public void kill(TopicDescriptor topicDescriptor) {
    	System.out.println("killing stream for topic: " + topicDescriptor);
    	StreamDescriptor streamDescriptor = streamDescriptors.remove(topicDescriptor);
    	streamDescriptor.getKillSwitch().shutdown();
    	for (TopicDescriptor descriptor : streamDescriptor.getTopicDescriptors()) {
    		streamDescriptors.remove(descriptor);
    	}
        System.out.println("remaining stream amount: " + streamDescriptors.size());
    }
    
    public void create(TopicDescriptor topicDescriptor) {
    	System.out.println("creating entity manager stream for topic " + topicDescriptor);
    	UUID uuid = UUID.randomUUID();
    	EntityManager entityManager = new EntityManager(uuid);
    	UniqueKillSwitch killSwitch = createSource(topicDescriptor)
    			.throttle(1, FiniteDuration.create(9, TimeUnit.SECONDS), 1, ThrottleMode.shaping())
    			.viaMat(KillSwitches.single(), Keep.right())
    			.via(Flow.fromFunction(EntitiesSupervisor::convertConsumerRecord))
    			.via(Flow.fromFunction(entityManager::apply))
    			.to(Sink.foreach(EntitiesSupervisor::dummySink))
    			.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	streamDescriptors.put(topicDescriptor, 
    			new StreamDescriptor(killSwitch, uuid, Arrays.asList(topicDescriptor)));
    }
    
    private void merge(List<TopicDescriptor> topicDescriptors) {
    	System.out.println("Killing previous streams");
    	for (TopicDescriptor descriptor : topicDescriptors) {
    		if (streamDescriptors.containsKey(descriptor)) {
    			kill(descriptor);
    		}
    	}
    	
    	System.out.println("creating merged stream");
    	Graph<SourceShape<ConsumerRecord<String, String>>, ?> source = GraphDSL.create(builder -> {
    		UniformFanInShape<ConsumerRecord<String, String>, ConsumerRecord<String, String>> merger = 
    				builder.add(Merge.create(topicDescriptors.size()));
    		for (TopicDescriptor descriptor : topicDescriptors) {
    			builder.from(builder.add(createSource(descriptor)).out()).toFanIn(merger);
    		}
    		
    		return SourceShape.of(merger.out());
    	});
    	UUID uuid = UUID.randomUUID();
    	EntityManager entityManager = new EntityManager(uuid);
    	UniqueKillSwitch killSwitch = Source.fromGraph(source)
    			.viaMat(KillSwitches.single(), Keep.right())
        		.via(Flow.fromFunction(EntitiesSupervisor::convertConsumerRecord))
        		.via(Flow.fromFunction(entityManager::apply))
        		.to(Sink.foreach(EntitiesSupervisor::dummySink))
        		.run(materializer);
    	
    	System.out.println("storing stream descriptor for later use");
    	StreamDescriptor streamDescriptor = new StreamDescriptor(killSwitch, uuid, topicDescriptors);
    	for (TopicDescriptor descriptor : topicDescriptors) {
    		streamDescriptors.put(descriptor, streamDescriptor);
    	}
    	
    }
    
    private Source<ConsumerRecord<String, String>, Consumer.Control> createSource(TopicDescriptor topicDescriptor) {
    	String topic = topicDescriptor.getSensorId() + "." + topicDescriptor.getReportsId();
    	System.out.println("creating source for topic " + topic);
    	return Consumer.plainSource(createConsumerSettings(), 
				Subscriptions.assignment(new TopicPartition(topic, 0)));
    }
    
    private ConsumerSettings<String, String> createConsumerSettings() {
    	return ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
				.withBootstrapServers("localhost:9092")
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
    
    private static EntityReport convertConsumerRecord(ConsumerRecord<String, String> record) {
    	System.out.println("converting record " + record);
    	String[] parts = record.value().split(",");
    	double longitude = Double.parseDouble(parts[0]);
    	double latitude = Double.parseDouble(parts[1]);
    	String nationality = parts[2];
    	return new EntityReport(new Coordinate(longitude, latitude), nationality);
    }
    
    private static void dummySink(EntityReport report) {
    	System.out.println("wrote report to sink: " + report);
    }

}
