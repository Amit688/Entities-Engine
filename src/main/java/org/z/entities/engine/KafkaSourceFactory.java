package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;
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
	/**
	 * Creates a source that listens on a topic.
	 * 
	 * @param system
	 * @param topic
	 * @return
	 */
	public static Source<ConsumerRecord<String, Object>, Consumer.Control> create(ActorSystem system, String topic) {
		ConsumerSettings<String, Object> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new KafkaAvroDeserializer())
    			.withBootstrapServers("localhost:9092")
    			.withGroupId("group1")
    			.withProperty("schema.registry.url", "https://schema-registry-ui.landoop.com")
    			.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return Consumer.plainSource(consumerSettings, 
        		Subscriptions.assignment(new TopicPartition(topic, 0)));
	}
	
	/**
	 * Creates a source that filters its messages by a reportId
	 * 
	 * @param system
	 * @param topic
	 * @param reportsId
	 * @return
	 */
	public static Source<ConsumerRecord<String, Object>, Consumer.Control> create(ActorSystem system, String topic, String reportsId) {
		return create(system, topic).filter(record -> filterByReportsId(record, reportsId));
	}
	
	private static boolean filterByReportsId(ConsumerRecord<String, Object> incomingUpdate, String reportsId) {
		GenericRecord data = (GenericRecord) incomingUpdate.value();
		return data.get("externalSystemID").equals(reportsId);
	}
	
	/**
	 * Creates a source from a source descriptor
	 * 
	 * @param system
	 * @param descriptor
	 * @return
	 */
	public static Source<ConsumerRecord<String, Object>, Consumer.Control> create(ActorSystem system, SourceDescriptor descriptor) {
		return create(system, descriptor.getSensorId(), descriptor.getReportsId());
	}
}
