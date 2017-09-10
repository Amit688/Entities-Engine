package org.z.entities.engine;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map; 
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap; 
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueue; 


/**
 * @author assafsh
 * Aug 2017
 * 
 * Each interface will have it's own BackOffice instance
 * that will hold a map with all the producer records in 
 * Map<String,Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>>> dataMap 
 * 
 * exteranlSystemID | Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>>
 *
 */
public class BackOffice implements java.util.function.Consumer<GenericRecord>,Closeable {

	private Map<String,Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>>> dataMap;
	private String sourceName;
	private KafkaProducer<Object, Object> producer;
	private boolean testing = Main.testing;
	private ActorSystem system;
	private Materializer materializer;


	public BackOffice(String sourceName,Materializer materializer,ActorSystem system) {

		dataMap = new ConcurrentHashMap<>();
		this.sourceName = sourceName;
		producer = new KafkaProducer<>(getProperties());
		this.system = system;
		this.materializer = materializer;
	}

	@Override
	public void accept(GenericRecord record) {

		System.out.println("BackOffice <"+sourceName+"> accept Message "+record);
		handleNewMessage(record); 		
	}

	@Override
	public void close() throws IOException {

		producer.close();		
	}

	public void updateTheSourceQueue(String externalSystemId,SourceQueue<GenericRecord> queue ) {

		if( dataMap.containsKey(externalSystemId) ) {

			dataMap.get(externalSystemId).setRight(queue);	
			ConcurrentLinkedQueue<GenericRecord> linkedQueue = dataMap.get(externalSystemId).getLeft();
			while(!linkedQueue.isEmpty()) {
				System.out.println("Offer from backlog queue in update Queue");
				queue.offer(linkedQueue.poll());				
			}			
		}	
		else {
			ConcurrentLinkedQueue<GenericRecord> linkedQueue = new ConcurrentLinkedQueue<>();
			Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>> pair = new Pair<>(linkedQueue,queue);
			dataMap.put(externalSystemId,pair);
		}
	}

	public void stopSourceQueueStream(String externalSystemId) {

		SourceQueue<GenericRecord> sourceQueue = dataMap.get(externalSystemId).getRight();
		deleteSourceQueue(externalSystemId);
		System.out.println("stopSourceQueueStream ExternalSystemID "+externalSystemId);
		sourceQueue.offer(getStopMeMessage());

	}

	private void deleteSourceQueue(String externalSystemId) {

		dataMap.get(externalSystemId).setRight(null);
	}



	private void handleNewMessage(GenericRecord record) {

		String externalSystemId = record.get("externalSystemID").toString(); 
		System.out.println("ExternalSystemID "+externalSystemId);
		if( !dataMap.containsKey(externalSystemId) ) {

			System.out.println("New externalSystemID");
			ConcurrentLinkedQueue<GenericRecord> linkedQueue = new ConcurrentLinkedQueue<>();
			linkedQueue.add(record);
			Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>> pair = new Pair<>(linkedQueue,null);
			dataMap.put(externalSystemId,pair);
			publishToCreationTopic(externalSystemId);
		}
		else {
			SourceQueue<GenericRecord>  sourceQueue = dataMap.get(externalSystemId).getRight();
			ConcurrentLinkedQueue<GenericRecord> linkedQueue = dataMap.get(externalSystemId).getLeft();

			if( sourceQueue != null ) {
				//Send the message to EntityManager but first send the backlog is exists
				while(!linkedQueue.isEmpty()) {
					System.out.println("Offer from backlog queue");
					sourceQueue.offer(linkedQueue.poll());
				}		
				System.out.println("Offer");
				sourceQueue.offer(record);
			}
			else {
				//The sourceQueue doesn't exist yet
				System.out.println("sourceQueue is missing yet, keep in queue");
				dataMap.get(externalSystemId).getLeft().add(record);				
			}
		}
	} 

	private void publishToCreationTopic(String externalSystemId) {

		ProducerRecord<Object, Object> sendRecord;
		try {
			if( testing ) {
				SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient(); 
				ProducerSettings<String, Object> producerSettings = ProducerSettings
						.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
						.withBootstrapServers("192.168.0.51:9092");

				Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);

				ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>("creation", getGenericRecordForCreation(externalSystemId));
				Source.from(Arrays.asList(producerRecord))
				.to(sink)
				.run(materializer);
			}
			else {
				sendRecord = new ProducerRecord<>("creation",getGenericRecordForCreation(externalSystemId));
				producer.send(sendRecord);
			}
		} catch (IOException | RestClientException e) {

			System.out.println("FAILED TO PUBLISH MESSAGE TO CREATION TOPIC");

		}		
	}

	private Properties getProperties() {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_ADDRESS"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroDeserializer.class);		
		props.put("schema.registry.url", System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		props.put("group.id", "group1");

		return props;
	}

	private GenericRecord getGenericRecordForCreation(String externalSystemID) throws IOException, RestClientException {

		Schema creationSchema;
		if(testing) {
			SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient(); 
			Schema.Parser parser = new Schema.Parser();
			schemaRegistry.register("detectionEvent",
					parser.parse("{\"type\": \"record\", "
							+ "\"name\": \"detectionEvent\", "
							+ "\"doc\": \"This is a schema for entity detection report event\", "
							+ "\"fields\": ["
							+ "{ \"name\": \"sourceName\", \"type\": \"string\", \"doc\" : \"interface name\" }, "
							+ "{ \"name\": \"externalSystemID\", \"type\": \"string\", \"doc\":\"external system ID\"},"
							+ "{ \"name\": \"dataOffset\", \"type\": \"long\", \"doc\":\"Data Offset\"}"
							+ "]}"));
			int id = schemaRegistry.getLatestSchemaMetadata("detectionEvent").getId();	
			creationSchema = schemaRegistry.getByID(id);
		}
		else {
			creationSchema = getSchema("detectionEvent");
		}

		GenericRecord creationRecord = new GenericRecordBuilder(creationSchema)
		.set("sourceName", sourceName)
		.set("externalSystemID",externalSystemID)
		.set("dataOffset",3333L)
		.build();

		return creationRecord;
	}

	private Schema getSchema(String name) throws IOException, RestClientException {

		String schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		String schemaRegistryIdentity = System.getenv("SCHEMA_REGISTRY_IDENTITY");		
		SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, Integer.parseInt(schemaRegistryIdentity));
		name = "org.z.entities.schema."+name;
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}

	private GenericRecord getStopMeMessage() {

		Schema dataSchema = null;
		Schema basicAttributesSchema = null;
		try {
			if(testing) {

				SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
				Schema.Parser parser = new Schema.Parser();
				schemaRegistry.register("basicEntityAttributes",
						parser.parse("{\"type\": \"record\","
								+ "\"name\": \"basicEntityAttributes\","
								+ "\"doc\": \"This is a schema for basic entity attributes, this will represent basic entity in all life cycle\","
								+ "\"fields\": ["
								+ "{\"name\": \"coordinate\", \"type\":"
								+ "{\"type\": \"record\","
								+ "\"name\": \"coordinate\","
								+ "\"doc\": \"Location attribute in grid format\","
								+ "\"fields\": ["
								+ "{\"name\": \"lat\",\"type\": \"double\"},"
								+ "{\"name\": \"long\",\"type\": \"double\"}"
								+ "]}},"
								+ "{\"name\": \"isNotTracked\",\"type\": \"boolean\"},"
								+ "{\"name\": \"entityOffset\",\"type\": \"long\"},"
								+ "{\"name\": \"sourceName\", \"type\": \"string\"}"
								+ "]}"));
				schemaRegistry.register("generalEntityAttributes",
						parser.parse("{\"type\": \"record\", "
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
				int id = schemaRegistry.getLatestSchemaMetadata("basicEntityAttributes").getId();
				basicAttributesSchema = schemaRegistry.getByID(id);

				id = schemaRegistry.getLatestSchemaMetadata("generalEntityAttributes").getId();
				dataSchema = schemaRegistry.getByID(id);

			}
			else {
				basicAttributesSchema = getSchema("basicEntityAttributes");
			}

			Schema coordinateSchema = basicAttributesSchema.getField("coordinate").schema();
			GenericRecord coordinate = new GenericRecordBuilder(coordinateSchema)
			.set("lat", 4.5d)
			.set("long", 3.4d)
			.build();
			GenericRecord basicAttributes = new GenericRecordBuilder(basicAttributesSchema)
			.set("coordinate", coordinate)
			.set("isNotTracked", false)
			.set("entityOffset", 50l)
			.set("sourceName", "source0")
			.build();

			Schema nationalitySchema = dataSchema.getField("nationality").schema();
			Schema categorySchema = dataSchema.getField("category").schema();
			GenericRecord dataRecord = new GenericRecordBuilder(dataSchema)
			.set("basicAttributes", basicAttributes)
			.set("speed", 4.7)
			.set("elevation", 7.8)
			.set("course", 8.3)
			.set("nationality", new GenericData.EnumSymbol(nationalitySchema, "USA"))
			.set("category", new GenericData.EnumSymbol(categorySchema, "boat"))
			.set("pictureURL", "huh?")
			.set("height", 6.1)
			.set("nickname", "rerere")
			.set("externalSystemID", "STOPME")
			.build();

			return dataRecord;

		} catch (IOException | RestClientException e) {

			e.printStackTrace();
		}

		return null;

	}



}

class Pair<L,R> {

	private L left;
	private R right;

	public Pair(L left, R right) {
		this.left = left;
		this.right = right;
	}

	public L getLeft() { 
		return left; 
	}

	public R getRight() { 
		return right; 
	}

	public void setLeft(L left) { 
		this.left = left; 
	}
	public void setRight(R right) {
		this.right = right;
	} 
}
