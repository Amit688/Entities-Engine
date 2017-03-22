package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Source;

public class KafkaSourceFactory {
	public static Source<ConsumerRecord<String, byte[]>, Consumer.Control> createRaw(ActorSystem system, String topic) {
		ConsumerSettings<String, byte[]> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new ByteArrayDeserializer())
    			.withBootstrapServers("localhost:9092")
    			.withGroupId("group1")
    			.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return Consumer.plainSource(consumerSettings, 
    				Subscriptions.assignment(new TopicPartition(topic, 0)));
	}
	
	public static Source<ConsumerRecord<String, GenericRecord>, Consumer.Control> create(ActorSystem system, String topic) {
        ConsumerSettings<String, GenericRecord> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new GenericRecordDeserializer())
    			.withBootstrapServers("localhost:9092")
    			.withGroupId("group1")
    			.withProperty("schema.registry.url", "https://schema-registry-ui.landoop.com")
    			.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return Consumer.plainSource(consumerSettings,
        		Subscriptions.assignment(new TopicPartition(topic, 0)));
	}
	
	public static Source<ConsumerRecord<String, GenericRecord>, Consumer.Control> create(ActorSystem system, TopicDescriptor descriptor) {
		return create(system, descriptor.getSensorId() + "." + descriptor.getReportsId());
	}
}
