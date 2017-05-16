package org.z.entities.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import akka.actor.ActorRef;
import akka.kafka.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

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
	private Map<String, Source<ConsumerRecord<Object, Object>, Consumer.Control>> topicSource;
	private Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> sink;
	private ActorRef consumerActor;
	
	public KafkaComponentsFactory(ActorSystem system, SchemaRegistryClient schemaRegistry, String kafkaUrl,
								  boolean sharingSources, boolean sharingSinks) {
		this.system = system;
		this.schemaRegistry = schemaRegistry;
		this.kafkaUrl = kafkaUrl;
		this.sharingSources = sharingSources;
		this.sharingSinks = sharingSinks;
		this.topicSource = new HashMap<>();
		if (this.sharingSources) {
			this.consumerActor = system.actorOf((KafkaConsumerActor.props(createConsumerSettings())));
		}
		if (this.sharingSinks) {
			this.sink = createNewSink();
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
	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic, String reportsId) {
		return getSource(topic).filter(record -> filterByReportsId(record, reportsId));
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
		return getSource(descriptor.getSensorId(), descriptor.getReportsId());
	}

	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic) {
		if (sharingSources) {
			return Consumer.plainExternalSource(consumerActor, Subscriptions.assignment((getTopicPartition(topic))));
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
			return Consumer.plainSource(createConsumerSettings(),
					(Subscription) Subscriptions.assignmentWithOffset(getTopicPartition(topic), offset));
		}
	}

	private ConsumerSettings<Object, Object> createConsumerSettings() {
		return ConsumerSettings.create(system, new KafkaAvroDeserializer(schemaRegistry),
				new KafkaAvroDeserializer(schemaRegistry))
        		.withBootstrapServers(kafkaUrl)
        		.withGroupId("group1")
        		.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	private TopicPartition getTopicPartition(String topic) {
		return new TopicPartition(topic, 0);
	}

	public Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> getSink() {
		if (sharingSinks){
			System.out.println("Sharing Sink!");
			return sink;
		} else {
			System.out.println("Creating new Sink");
			return createNewSink();
		}
	}

	public Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> createNewSink() {
		ProducerSettings<Object, Object> producerSettings = ProducerSettings
				.create(system, new KafkaAvroSerializer(schemaRegistry), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(kafkaUrl);
		return Producer.plainSink(producerSettings);
	}
}
