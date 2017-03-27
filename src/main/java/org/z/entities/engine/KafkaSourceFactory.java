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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class KafkaSourceFactory {
	private ActorSystem system;
	private SchemaRegistryClient schemaRegistry;
	
	public KafkaSourceFactory(ActorSystem system, SchemaRegistryClient schemaRegistry) {
		this.system = system;
		this.schemaRegistry = schemaRegistry;
	}
	
	/**
	 * Creates a source that listens on a topic.
	 * 
	 * @param system
	 * @param topic
	 * @return
	 */
	public Source<ConsumerRecord<String, Object>, Consumer.Control> create(String topic) {
		ConsumerSettings<String, Object> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new KafkaAvroDeserializer(schemaRegistry))
    			.withBootstrapServers(System.getenv("KAFKA_ADDRESS"))
    			.withGroupId("group1")
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
	public Source<ConsumerRecord<String, Object>, Consumer.Control> create(String topic, String reportsId) {
		return create(topic).filter(record -> filterByReportsId(record, reportsId));
	}
	
	private boolean filterByReportsId(ConsumerRecord<String, Object> incomingUpdate, String reportsId) {
		System.out.println("checking");
		GenericRecord data = (GenericRecord) incomingUpdate.value();
		return data.get("externalSystemID").toString().equals(reportsId);
	}
	
	/**
	 * Creates a source from a source descriptor
	 * 
	 * @param system
	 * @param descriptor
	 * @return
	 */
	public Source<ConsumerRecord<String, Object>, Consumer.Control> create(SourceDescriptor descriptor) {
		return create(descriptor.getSensorId(), descriptor.getReportsId());
	}
}
