package sample.kafka;

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

public class AkkaKafka {

	public static void main(String[] args) throws InterruptedException {
		// akka
		ActorSystem system = ActorSystem.create();
		ActorMaterializer materializer = ActorMaterializer.create(system);
		ConsumerSettings<String, String> consumerSettings = ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
				.withBootstrapServers("localhost:9092")
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		Source<ConsumerRecord<String, String>, Consumer.Control> source = 
				Consumer.plainSource(consumerSettings, 
						Subscriptions.assignment(new TopicPartition("test", 0)));
		source
			.to(Sink.foreach(record -> System.out.println(record.value())))
			.run(materializer);
		
		Thread.sleep(3000);
		System.out.println("stopping");
		system.terminate();
	}

}
