package org.z.entities.engine;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import akka.actor.ActorRef;
import akka.kafka.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.Done;
import akka.actor.ActorSystem;
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
	private final boolean sharingSources;
	private final boolean sharingSinks;
	private ActorRef consumerActor;
	private KafkaProducer<Object, Object> kafkaProducer = null;
	
	public KafkaComponentsFactory(ActorSystem system, SchemaRegistryClient schemaRegistry, String kafkaUrl,
								  boolean sharingSources, boolean sharingSinks) {
		this.system = system;
		this.schemaRegistry = schemaRegistry;
		this.kafkaUrl = kafkaUrl;
		this.sharingSources = sharingSources;
		this.sharingSinks = sharingSinks;
		if (this.sharingSources) {
			this.consumerActor = system.actorOf((KafkaConsumerActor.props(createConsumerSettings())));
		}
		if (this.sharingSinks) {
			ProducerSettings<Object, Object> producerSettings = createProducerSettings();
			this.kafkaProducer = producerSettings.createKafkaProducer();
		}
	}

	/**
	 * Creates a source that filters its messages by a reportId
	 *
	 * @param system
	 * @param topic
	 * @param reportsId
	 * @return
	 */
	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic, String reportsId,long dataOffset) {
		return getSource(topic,dataOffset).filter(record -> filterByReportsId(record, reportsId));
	}

	private boolean filterByReportsId(ConsumerRecord<Object, Object> incomingUpdate, String reportsId) {
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
	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(SourceDescriptor descriptor) {
		return getSource(descriptor.getSensorId(), descriptor.getReportsId(),descriptor.getDataOffset());
	}

	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic) {
		if (sharingSources) {
 
			return Consumer.plainExternalSource(consumerActor, Subscriptions.assignment(new TopicPartition(topic, 0)));
		} else {
			return Consumer.plainSource(createConsumerSettings(),
					(Subscription) Subscriptions.assignment(getTopicPartition(topic)));
		}
	}

	private Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic, long offset) {
		if (sharingSources) {
			return Consumer.plainExternalSource(consumerActor,
					Subscriptions.assignmentWithOffset(getTopicPartition(topic), offset));
		} else {
			System.out.println("Topic "+topic+" offset "+offset);
			return Consumer.plainSource(createConsumerSettings(),
					(Subscription) Subscriptions.assignmentWithOffset(getTopicPartition(topic), offset));
		}
	}

	private ConsumerSettings<Object, Object> createConsumerSettings() {
		KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer(schemaRegistry);
		
		keyDeserializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), true);
		return ConsumerSettings.create(system, keyDeserializer,
				new KafkaAvroDeserializer(schemaRegistry))
        		.withBootstrapServers(kafkaUrl)
        		.withGroupId("group1")
        		.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	private TopicPartition getTopicPartition(String topic) {
		return new TopicPartition(topic, 0);
	}

	public Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> getSink() {
		ProducerSettings<Object, Object> producerSettings = createProducerSettings();
		if (sharingSinks){
			return Producer.plainSink(producerSettings, kafkaProducer);
		} else {
			return Producer.plainSink(producerSettings);
		}
	}

	private ProducerSettings<Object, Object> createProducerSettings() {
		KafkaAvroSerializer keySerializer = new KafkaAvroSerializer(schemaRegistry);
		keySerializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), true);
		return ProducerSettings
                    .create(system, keySerializer, new KafkaAvroSerializer(schemaRegistry))
                    .withBootstrapServers(kafkaUrl);
	}
	
}
