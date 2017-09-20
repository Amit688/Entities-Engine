package org.z.entities.engine;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * A collection of methods that write data to the system
 */
public class Simulator {
	public static void writeSomeData(ActorSystem system, Materializer materializer,
			SchemaRegistryClient schemaRegistry, EntitiesSupervisor supervisor)
					throws IOException, RestClientException {
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers("192.168.0.51:9092");
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);

		Schema creationSchema = getSchema(schemaRegistry, "DetectionEvent");
		GenericRecord creationRecord = new GenericRecordBuilder(creationSchema)
		.set("sourceName", "source1")
		.set("externalSystemID", "id1")
		.set("dataOffset", 444L)
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

		Schema basicAttributesSchema = getSchema(schemaRegistry, "BasicEntityAttributes");
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
		Schema dataSchema = getSchema(schemaRegistry, "GeneralEntityAttributes");
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


		Set<UUID> uuidSet = supervisor.getAllUuids();
		UUID[] array = uuidSet.stream().toArray(UUID[]::new);

		Schema mergeSchema = getSchema(schemaRegistry, "MergeEvent");
		GenericRecord mergeRecord = new GenericRecordBuilder(mergeSchema) 
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

		uuidSet = supervisor.getAllUuids();
		array = uuidSet.stream().toArray(UUID[]::new);

		Schema splitSchema = getSchema(schemaRegistry, "SplitEvent");
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

	public static void writeSomeDataForMailRoom(ActorSystem system, Materializer materializer,
			SchemaRegistryClient schemaRegistry,
			KafkaComponentsFactory componentsFactory)
					throws IOException, RestClientException {
		
		for(int i = 0 ; i < 1; i++) {

			Schema basicAttributesSchema = getSchema(schemaRegistry, "BasicEntityAttributes");
			Schema coordinateSchema = basicAttributesSchema.getField("coordinate").schema();
			long x = i;
			GenericRecord coordinate = new GenericRecordBuilder(coordinateSchema)
			.set("lat", 4.5d+x)
			.set("long", 3.4d+x)
			.build();
			GenericRecord basicAttributes = new GenericRecordBuilder(basicAttributesSchema)
			.set("coordinate", coordinate)
			.set("isNotTracked", false)
			.set("entityOffset", 50l)
			.set("sourceName", "source1")
			.build();
			Schema dataSchema = getSchema(schemaRegistry, "GeneralEntityAttributes");
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
			.set("externalSystemID", "id1_source1")
			.build();

			ProducerRecord<Object, Object> producerRecord3 = new ProducerRecord("source1", dataRecord);
			componentsFactory.getKafkaProducer().send(producerRecord3);

			try {
				Thread.sleep(2000);
			} catch (Exception e) {

			}

		}

		for(int i = 0 ; i < 1; i++) {

			Schema basicAttributesSchema = getSchema(schemaRegistry, "BasicEntityAttributes");
			Schema coordinateSchema = basicAttributesSchema.getField("coordinate").schema();
			long x = i;
			GenericRecord coordinate = new GenericRecordBuilder(coordinateSchema)
			.set("lat", 4.5d+x)
			.set("long", 3.4d+x)
			.build();
			GenericRecord basicAttributes = new GenericRecordBuilder(basicAttributesSchema)
			.set("coordinate", coordinate)
			.set("isNotTracked", false)
			.set("entityOffset", 50l)
			.set("sourceName", "source0")
			.build();
			Schema dataSchema = getSchema(schemaRegistry, "GeneralEntityAttributes");
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
			.set("externalSystemID", "id1_source_0")
			.build();

			ProducerRecord<Object, Object> producerRecord3 = new ProducerRecord("source0", dataRecord);
			componentsFactory.getKafkaProducer().send(producerRecord3);

			try {
				Thread.sleep(2000);
			} catch (Exception e) {

			}

		}
	}

	public static void writeMerge(ActorSystem system, Materializer materializer,
			SchemaRegistryClient schemaRegistry, Collection<UUID> entitiesToMerge) {
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(System.getenv("KAFKA_ADDRESS"));
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);

		try {
			List<String> asStrings = new ArrayList<>(entitiesToMerge.size());
			entitiesToMerge.forEach(uuid -> asStrings.add(uuid.toString()));
			Schema schema = getSchema(schemaRegistry, "mergeEvent");
			GenericRecord mergeMessage = new GenericRecordBuilder(schema)
			.set("mergedEntitiesId", asStrings)
			.build();

			ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("merge", mergeMessage);
			System.out.println("merge message: " + producerRecord);

			Source.from(Arrays.asList(producerRecord))
			.to(sink)
			.run(materializer);
		} catch (IOException | RestClientException e) {
			throw new RuntimeException(e);
		}

	}

	public static void writeSplit(ActorSystem system, Materializer materializer,
			SchemaRegistryClient schemaRegistry, UUID entityToSplit) {
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(System.getenv("KAFKA_ADDRESS"));
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);

		try {
			Schema schema = getSchema(schemaRegistry, "SplitEvent");
			GenericRecord splitMessage = new GenericRecordBuilder(schema)
			.set("splittedEntityID", entityToSplit.toString())
			.build();

			ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("split", splitMessage);
			System.out.println("split message: " + producerRecord);

			Source.from(Arrays.asList(producerRecord))
			.to(sink)
			.run(materializer);
		} catch (IOException | RestClientException e) {
			throw new RuntimeException(e);
		}
	}

	private static Schema getSchema(SchemaRegistryClient schemaRegistry, String name) throws IOException, RestClientException {
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}
}
