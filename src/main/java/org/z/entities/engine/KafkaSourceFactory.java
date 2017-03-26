package org.z.entities.engine;

import java.io.IOException;

import org.apache.avro.Schema;
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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class KafkaSourceFactory {
	
	// package private for testing
	static Schema.Parser parser = new Schema.Parser();
	static SchemaRegistryClient schemaRegistry = createSchemaRegistry();
	/**
	 * Creates a source that listens on a topic.
	 * 
	 * @param system
	 * @param topic
	 * @return
	 */
	public static Source<ConsumerRecord<String, Object>, Consumer.Control> create(ActorSystem system, String topic) {
		ConsumerSettings<String, Object> consumerSettings = 
    			ConsumerSettings.create(system, new StringDeserializer(), new KafkaAvroDeserializer(schemaRegistry))
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
	public static Source<ConsumerRecord<String, Object>, Consumer.Control> create(ActorSystem system, SourceDescriptor descriptor) {
		return create(system, descriptor.getSensorId(), descriptor.getReportsId());
	}
	
	private static SchemaRegistryClient createSchemaRegistry() {
		try {
		SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
		schemaRegistry.register("basicEntityAttributes", parser.parse(
				"{\"type\": \"record\","
				+ "\"name\": \"basicEntityAttributes\","
				+ "\"doc\": \"This is a schema for basic entity attributes, this will represent basic entity in all life cycle\","
				+ "\"fields\": ["
					+ "{\"name\": \"coordinate\","
					+ "\"type\":"
						+ "{\"type\": \"record\","
						+ "\"name\": \"coordinate\","
						+ "\"doc\": \"Location attribute in grid format\","
						+ "\"fields\": ["
							+ "{\"name\": \"lat\",\"type\": \"double\"},"
							+ "{\"name\": \"long\",\"type\": \"double\"}"
						+ "]}},"
					+ "{\"name\": \"isNotTracked\",\"type\": \"boolean\"},"
					+ "{\"name\": \"entityOffset\",\"type\": \"long\"}"
				+ "]}"));
		schemaRegistry.register("generalEntityAttributes", parser.parse(
				"{\"type\": \"record\", "
				+ "\"name\": \"generalEntityAttributes\","
				+ "\"doc\": \"This is a schema for general entity before acquiring by the system\","
				+ "\"fields\": ["
				+ "{\"name\": \"basicAttributes\",\"type\": \"basicEntityAttributes\"},"
				+ "{\"name\": \"speed\",\"type\": \"double\",\"doc\" : \"This is the magnitude of the entity's velcity vector.\"},"
				+ "{\"name\": \"elevation\",\"type\": \"double\"},"
				+ "{\"name\": \"course\",\"type\": \"double\"},"
				+ "{\"name\": \"nationality\",\"type\": {\"name\": \"nationality\", \"type\": \"enum\",\"symbols\" : [\"ISRAEL\", \"USA\", \"SPAIN\"]}},"
				+ "{\"name\": \"category\",\"type\": {\"name\": \"category\", \"type\": \"enum\",\"symbols\" : [\"airplane\", \"boat\"]}},"
				+ "{\"name\": \"pictureURL\",\"type\": \"string\"},"
				+ "{\"name\": \"height\",\"type\": \"double\"},"
				+ "{\"name\": \"nickname\",\"type\": \"string\"},"
				+ "{\"name\": \"externalSystemID\",\"type\": \"string\",\"doc\" : \"This is ID given be external system.\"}"
				+ "]}"));
		return schemaRegistry;
		} catch (RestClientException | IOException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
