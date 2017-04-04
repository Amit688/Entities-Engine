package org.z.entities.engine;

import java.util.concurrent.CompletionStage;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KafkaComponentsFactory {
	private ActorSystem system;
	private SchemaRegistryClient schemaRegistry;
	private String kafkaUrl;
	
	public KafkaComponentsFactory(ActorSystem system, SchemaRegistryClient schemaRegistry, String kafkaUrl) {
		this.system = system;
		this.schemaRegistry = schemaRegistry;
		this.kafkaUrl = kafkaUrl;
	}
	
	/**
	 * Creates a source that listens on a topic.
	 * 
	 * @param system
	 * @param topic
	 * @return
	 */
	public Source<ConsumerRecord<String, Object>, Consumer.Control> createSource(String topic) {
		ConsumerSettings<String, Object> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new KafkaAvroDeserializer(schemaRegistry))
    			.withBootstrapServers(kafkaUrl)
    			.withGroupId("group1")
    			.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return Consumer.plainSource(consumerSettings, 
        		Subscriptions.assignment(new TopicPartition(topic, 0)));
	}
	
	public Source<ConsumerRecord<String, Object>, Consumer.Control> createSource(String topic, long offset) {
		ConsumerSettings<String, Object> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new KafkaAvroDeserializer(schemaRegistry))
    			.withBootstrapServers(kafkaUrl)
    			.withGroupId("group1")
    			.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return Consumer.plainSource(consumerSettings, 
        		Subscriptions.assignmentWithOffset(new TopicPartition(topic, 0), offset));
	}
	
	/**
	 * Creates a source that filters its messages by a reportId
	 * 
	 * @param system
	 * @param topic
	 * @param reportsId
	 * @return
	 */
	public Source<ConsumerRecord<String, Object>, Consumer.Control> createSource(String topic, String reportsId) {
		return createSource(topic).filter(record -> filterByReportsId(record, reportsId));
	}
	
	private boolean filterByReportsId(ConsumerRecord<String, Object> incomingUpdate, String reportsId) {
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
	public Source<ConsumerRecord<String, Object>, Consumer.Control> createSource(SourceDescriptor descriptor) {
		return createSource(descriptor.getSensorId(), descriptor.getReportsId());
	}
	
	public Sink<ProducerRecord<String, Object>, CompletionStage<Done>> createSink() {
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(kafkaUrl);
		return Producer.plainSink(producerSettings);
	}
}
