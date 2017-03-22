package org.z.entities.engine;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.ThrottleMode;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.concurrent.duration.FiniteDuration;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
    	final ActorSystem system = ActorSystem.create();
    	Thread.sleep(2000);
		final ActorMaterializer materializer = ActorMaterializer.create(system);
		
		Source<ConsumerRecord<String, String>, Consumer.Control> source = createSource(system);
		EntitiesSupervisor supervisor = new EntitiesSupervisor(system, materializer);
		
		//testing
		source = source.throttle(1, FiniteDuration.create(10, TimeUnit.SECONDS), 1, ThrottleMode.shaping());
		//
		
		source
			.to(Sink.foreach(record -> supervisor.accept(record.value())))
			.run(materializer);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				system.terminate();
			}
		});
		System.out.println("Ready");
		while(true) {
			Thread.sleep(3000);
		}
    }
    
    private static Source<ConsumerRecord<String, String>, Consumer.Control> createSource(
    		ActorSystem system) {
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
