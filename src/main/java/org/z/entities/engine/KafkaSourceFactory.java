package org.z.entities.engine;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Source;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class KafkaSourceFactory {
	public static Source<ConsumerRecord<String, Object>, ?> create(ActorSystem system, String topic) {
        ConsumerSettings<String, Object> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new KafkaAvroDeserializer())
    			.withBootstrapServers("localhost:9092")
    			.withGroupId("group1")
    			.withProperty("schema.registry.url", "https://schema-registry-ui.landoop.com")
    			.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return Consumer.plainSource(consumerSettings,
        		Subscriptions.assignment(new TopicPartition(topic, 0)));
	}
	
	public static Source<ConsumerRecord<String, Object>, ?> create(ActorSystem system, TopicDescriptor descriptor) {
		return create(system, descriptor.getSensorId() + "." + descriptor.getReportsId());
	}
}
