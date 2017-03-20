package org.z.entities.engine;

import java.util.HashMap;
import java.util.Map;

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
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements java.util.function.Consumer<String> {
    ActorSystem system;
    Materializer materializer;
    Map<String, UniqueKillSwitch> killSwitches;
    
    public EntitiesSupervisor(ActorSystem system, Materializer materializer) {
    	this.system = system;
        this.materializer = materializer;
        killSwitches = new HashMap<>();
    }

    @Override
    public void accept(String message) {
    	String[] parts = message.split(",");
    	String action = parts[0];
    	String topic = parts[1];
    	
    	switch (action) {
		case "create":
			create(topic);
			break;
		case "kill":
			kill(topic);
			break;
		default:
			System.out.println("received unknown action " + action);
			break;
		}
    }
    
    public void kill(String key) {
    	System.out.println("killing stream with key " + key);
        killSwitches.remove(key).shutdown();
        System.out.println("remaining killswitch amount: " + killSwitches.size());
    }
    
    public void create(String topic) {
    	System.out.println("creating entity manager stream for topic " + topic);
    	UniqueKillSwitch killSwitch = createSource(topic)
    		.viaMat(KillSwitches.single(), Keep.right())
    		.via(Flow.fromFunction(EntitiesSupervisor::convertConsumerRecord))
    		.via(Flow.fromFunction(new EntityManager()::apply))
    		.to(Sink.foreach(EntitiesSupervisor::dummySink))
    		.run(materializer);
    	
    	System.out.println("storing killswitch for later use");
    	killSwitches.put(topic, killSwitch);
    }
    
    private Source<ConsumerRecord<String, String>, Consumer.Control> createSource(String topic) {
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
    	String[] parts = record.value().split(",");
    	double longitude = Double.parseDouble(parts[0]);
    	double latitude = Double.parseDouble(parts[1]);
    	String nationality = parts[3];
    	return new EntityReport(new Coordinate(longitude, latitude), nationality);
    }
    
    private static void dummySink(EntityReport report) {
    	System.out.println("wrote report to sink: " + report);
    }

}
