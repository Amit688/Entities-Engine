package org.z.entities.engine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source; 
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kamon.Kamon;
/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

	public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
		System.out.println("KAFKA_ADDRESS::::::::" + System.getenv("KAFKA_ADDRESS"));
		System.out.println("SCHEMA_REGISTRY_ADDRESS::::::::" + System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		System.out.println("SCHEMA_REGISTRY_IDENTITY::::::::" + System.getenv("SCHEMA_REGISTRY_IDENTITY"));
		System.out.println("SINGLE_SOURCE_PER_TOPIC::::::::" + System.getenv("SINGLE_SOURCE_PER_TOPIC"));
		System.out.println("SINGLE_SINK::::::::" + System.getenv("SINGLE_SINK"));
		System.out.println("KAMON_ENABLED::::::::" + System.getenv("KAMON_ENABLED"));
		System.out.println("CONF_IND::::::::" + System.getenv("CONF_IND"));

		boolean isKamonEnabled = Boolean.parseBoolean(System.getenv("KAMON_ENABLED"));

		final ActorSystem system;
		
		if(System.getenv("CONF_IND").equalsIgnoreCase("true")) {
	        	Config cfg = ConfigFactory.parseResources(Main.class, "/akka-streams.conf").resolve();
	       		 system = ActorSystem.create("sys", cfg);	
		}
		else {
			system = ActorSystem.create();
		}
		final ActorMaterializer materializer = ActorMaterializer.create(system);
		final SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(System.getenv("SCHEMA_REGISTRY_ADDRESS"), Integer.parseInt(System.getenv("SCHEMA_REGISTRY_IDENTITY")));
		final KafkaComponentsFactory sourceFactory = new KafkaComponentsFactory(system, schemaRegistry,
				System.getenv("KAFKA_ADDRESS"), Boolean.parseBoolean(System.getenv("SINGLE_SOURCE_PER_TOPIC")),
				Boolean.parseBoolean(System.getenv("SINGLE_SINK")));

		if (isKamonEnabled) {
			Kamon.start();
		}

		/* To run local via eclipse

		final ActorSystem system = ActorSystem.create();
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        registerSchemas(schemaRegistry);
        final KafkaComponentsFactory sourceFactory = new KafkaComponentsFactory(system, schemaRegistry,
                        "192.168.0.51:9092", false,false);

		 */

		EntitiesSupervisor supervisor = createSupervisorStream(materializer, sourceFactory);
		//writeSomeData(system, materializer, schemaRegistry,supervisor);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				system.terminate();
				if (isKamonEnabled) {
					Kamon.shutdown();
				}
			}
		});
		System.out.println("Ready");
		while(true) {
			Thread.sleep(3000);
		}
	}

	private static EntitiesSupervisor createSupervisorStream(ActorMaterializer materializer, KafkaComponentsFactory sourceFactory) {
		Source<EntitiesEvent, ?> detectionsSource = createSourceWithType(sourceFactory, "creation", EntitiesEvent.Type.CREATE);
		Source<EntitiesEvent, ?> mergesSource = createSourceWithType(sourceFactory, "merge", EntitiesEvent.Type.MERGE);
		Source<EntitiesEvent, ?> splitsSource = createSourceWithType(sourceFactory, "split", EntitiesEvent.Type.SPLIT);
		Source<EntitiesEvent, ?> combinedSource = Source.fromGraph(GraphDSL.create(builder -> {
			UniformFanInShape<EntitiesEvent, EntitiesEvent> merger = builder.add(Merge.create(3));
			directToMerger(builder, detectionsSource, merger);
			directToMerger(builder, mergesSource, merger);
			directToMerger(builder, splitsSource, merger);
			return SourceShape.of(merger.out());
		}));

		EntitiesSupervisor supervisor = new EntitiesSupervisor(materializer, sourceFactory);
		combinedSource
		.to(Sink.foreach(supervisor::accept))
		.run(materializer);

		return supervisor;
	}

	private static Source<EntitiesEvent, ?> createSourceWithType(KafkaComponentsFactory sourceFactory, 
			String topic, EntitiesEvent.Type type) {
		return sourceFactory.getSource(topic)
				.via(Flow.fromFunction(r -> new EntitiesEvent(type, (GenericRecord) r.value())));
	}

	private static void directToMerger(Builder<NotUsed> builder, 
			Source<EntitiesEvent, ?> source, UniformFanInShape<EntitiesEvent, ?> merger) {
		builder.from(builder.add(source).out()).toFanIn(merger);
	}

	private static void writeSomeData(ActorSystem system, Materializer materializer, 
			SchemaRegistryClient schemaRegistry, EntitiesSupervisor supervisor) throws IOException, RestClientException {
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers("192.168.0.51:9092");
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);

		Schema creationSchema = getSchema(schemaRegistry, "detectionEvent");
		GenericRecord creationRecord = new GenericRecordBuilder(creationSchema)
		.set("sourceName", "source1")
		.set("externalSystemID", "id1")
		.build();
		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>("creation", creationRecord);
		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer);

		GenericRecord creationRecord2 = new GenericRecordBuilder(creationSchema)
		.set("sourceName", "source2")
		.set("externalSystemID", "id1")
		.build();
		producerRecord = new ProducerRecord<String, Object>("creation", creationRecord2);
		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer);

		Schema basicAttributesSchema = getSchema(schemaRegistry, "basicEntityAttributes");
		Schema coordinateSchema = basicAttributesSchema.getField("coordinate").schema();
		GenericRecord coordinate = new GenericRecordBuilder(coordinateSchema)
		.set("lat", 4.5d)
		.set("long", 3.4d)
		.build();
		GenericRecord basicAttributes = new GenericRecordBuilder(basicAttributesSchema)
		.set("coordinate", coordinate)
		.set("isNotTracked", false)
		.set("entityOffset", 50l)
		.set("sourceName", "source1")
		.build();
		Schema dataSchema = getSchema(schemaRegistry, "generalEntityAttributes");
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
		.set("externalSystemID", "id1")
		.build();
		ProducerRecord<String, Object> producerRecord2 = new ProducerRecord<String, Object>("source1", dataRecord);

		Source.from(Arrays.asList(producerRecord2))
		.to(sink)
		.run(materializer);

		producerRecord2 = new ProducerRecord<String, Object>("source2", dataRecord);

		Source.from(Arrays.asList(producerRecord2))
		.to(sink)
		.run(materializer);

		try {
			Thread.sleep(10000);
		} catch (Exception e) {

		}


		Set<UUID> uuidSet = supervisor.getStreams().keySet();
		UUID[] array = uuidSet.stream().toArray(UUID[]::new);

		Schema mergeSchema = getSchema(schemaRegistry, "mergeEvent");
		GenericRecord mergeRecord = new GenericRecordBuilder(mergeSchema)
		//.set("mergedEntitiesId", Arrays.asList("38400000-8cf0-11bd-b23f-0b96e4ef00e1",
		//		"38400000-8cf0-11bd-b23f-0b96e4ef00e2"))
		.set("mergedEntitiesId", Arrays.asList(array[0].toString(), array[1].toString()))
		.build();
		producerRecord = new ProducerRecord<String, Object>("merge", mergeRecord);
		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer);

		try {
			Thread.sleep(10000);
		} catch (Exception e) {

		}

		producerRecord2 = new ProducerRecord<String, Object>("source1", dataRecord);

		Source.from(Arrays.asList(producerRecord2))
		.to(sink)
		.run(materializer);

		producerRecord2 = new ProducerRecord<String, Object>("source1", dataRecord);

		Source.from(Arrays.asList(producerRecord2))
		.to(sink)
		.run(materializer);


		try {
			Thread.sleep(10000);
		} catch (Exception e) {

		}

		uuidSet = supervisor.getStreams().keySet();
		array = uuidSet.stream().toArray(UUID[]::new);

		Schema splitSchema = getSchema(schemaRegistry, "splitEvent");
		GenericRecord splitRecord = new GenericRecordBuilder(splitSchema)
		//.set("splittedEntityID", "38400000-8cf0-11bd-b23f-0b96e4ef00e1")
		.set("splittedEntityID", array[0].toString())
		.build();
		producerRecord = new ProducerRecord<String, Object>("split", splitRecord);
		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer);

		try {
			Thread.sleep(5000);
		} catch (Exception e) {

		}

		producerRecord2 = new ProducerRecord<String, Object>("source1", dataRecord);

		Source.from(Arrays.asList(producerRecord2))
		.to(sink)
		.run(materializer);

		producerRecord2 = new ProducerRecord<String, Object>("source1", dataRecord);

		Source.from(Arrays.asList(producerRecord2))
		.to(sink)
		.run(materializer);

	}

	private static Schema getSchema(SchemaRegistryClient schemaRegistry, String name) throws IOException, RestClientException {
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}

	private static SchemaRegistryClient initializeSchemaRegistry() {
		Schema.Parser parser = new Schema.Parser();
		try {
			SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
			schemaRegistry.register("detectionEvent", 
					parser.parse("{\"type\": \"record\", "
							+ "\"name\": \"detectionEvent\", "
							+ "\"doc\": \"This is a schema for entity detection report event\", " 
							+ "\"fields\": ["
							+ "{ \"name\": \"sourceName\", \"type\": \"string\", \"doc\" : \"interface name\" }, " 
							+ "{ \"name\": \"externalSystemID\", \"type\": \"string\", \"doc\":\"external system ID\"},"
							+ "{ \"name\": \"dataOffset\", \"type\": \"long\", \"doc\":\"Data Offset\"}"
							+ "]}"));
			schemaRegistry.register("mergeEvent",
					parser.parse("{\"type\": \"record\", "
							+ "\"name\": \"mergeEvent\", "
							+ "\"doc\": \"This is a schema for merge entities event\", "
							+ "\"fields\": ["
							+ "{ \"name\": \"mergedEntitiesId\", \"type\":\n" +
							"    \t{\n" +
							"      \t\"type\": \"array\",\n" +
							"      \t\"items\": {\n" +
							"      \t\"name\": \"entityId\",\n" +
							"      \t\"type\": \"string\"\n" +
							"      \t}\n" +
							"  \t}}"
							+ "]}"));
			schemaRegistry.register("splitEvent",
					parser.parse("{\n" +
							"  \"type\": \"record\",\n" +
							"  \"name\": \"splitEvent\",\n" +
							"  \"fields\": [\n" +
							"\t{\n" +
							"  \t\"name\": \"splittedEntityID\",\n" +
							"  \t\"type\": \"string\"\n" +
							"\t}\n" +
							" ] \n" +
							"}"));
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
			schemaRegistry.register("systemEntity", 
					parser.parse("{\"type\": \"record\", "
							+ "\"name\": \"systemEntity\","
							+ "\"doc\": \"This is a schema of a single processed entity with all attributes.\","
							+ "\"fields\": ["
							+ "{\"name\": \"entityID\", \"type\": \"string\"}, "
							+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"}"
							+ "]}"));
			schemaRegistry.register("entityFamily", 
					parser.parse("{\"type\": \"record\", "
							+ "\"name\": \"entityFamily\", "
							+ "\"doc\": \"This is a schema of processed entity with full attributes.\","
							+ "\"fields\": ["
							+ "{\"name\": \"entityID\", \"type\": \"string\"},"
							+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"},"
							+ "{\"name\" : \"sons\", \"type\": [{\"type\": \"array\", \"items\": \"systemEntity\"}]}"
							+ "]}"));
			return schemaRegistry;
		} catch (RestClientException | IOException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	private static void registerSchemas(SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
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
		schemaRegistry.register("systemEntity", 
				parser.parse("{\"type\": \"record\", "
						+ "\"name\": \"systemEntity\","
						+ "\"doc\": \"This is a schema of a single processed entity with all attributes.\","
						+ "\"fields\": ["
						+ "{\"name\": \"entityID\", \"type\": \"string\"}, "
						+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"}"
						+ "]}"));
		schemaRegistry.register("entityFamily", 
				parser.parse("{\"type\": \"record\", "
						+ "\"name\": \"entityFamily\", "
						+ "\"doc\": \"This is a schema of processed entity with full attributes.\","
						+ "\"fields\": ["
						+ "{\"name\": \"entityID\", \"type\": \"string\"},"
						+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"},"
						+ "{\"name\" : \"sons\", \"type\": [{\"type\": \"array\", \"items\": \"systemEntity\"}]}"
						+ "]}"));

		schemaRegistry.register("mergeEvent",
				parser.parse("{\"type\": \"record\", "
						+ "\"name\": \"mergeEvent\", "
						+ "\"doc\": \"This is a schema for merge entities event\", "
						+ "\"fields\": ["
						+ "{ \"name\": \"mergedEntitiesId\", \"type\":\n" +
						"    \t{\n" +
						"      \t\"type\": \"array\",\n" +
						"      \t\"items\": {\n" +
						"      \t\"name\": \"entityId\",\n" +
						"      \t\"type\": \"string\"\n" +
						"      \t}\n" +
						"  \t}}"
						+ "]}"));

		schemaRegistry.register("splitEvent",
				parser.parse("{\n" +
						"  \"type\": \"record\",\n" +
						"  \"name\": \"splitEvent\",\n" +
						"  \"fields\": [\n" +
						"\t{\n" +
						"  \t\"name\": \"splittedEntityID\",\n" +
						"  \t\"type\": \"string\"\n" +
						"\t}\n" +
						" ] \n" +
						"}"));
	}
}
