package org.z.entities.engine;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

    public static void main(String[] args) {
    	final ActorSystem system = ActorSystem.create();
		final ActorMaterializer materializer = ActorMaterializer.create(system);
		
		Source<ConsumerRecord<String, String>, Consumer.Control> source = createSource(system, materializer);
		EntitiesSupervisor supervisor = new EntitiesSupervisor(system, materializer);
		
		source
			.to(Sink.foreach(record -> supervisor.accept(record.value())))
			.run(materializer);
    }
    
    private static Source<ConsumerRecord<String, String>, Consumer.Control> createSource(
    		ActorSystem system, ActorMaterializer materializer) {
    	return Consumer.plainSource(createConsumerSettings(system), 
				Subscriptions.assignment(new TopicPartition("test", 0)));
    }
    
    private static ConsumerSettings<String, String> createConsumerSettings(ActorSystem system) {
    	return ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
				.withBootstrapServers("localhost:9092")
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
}
