package org.z.entities.engine;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {
	
    public static void main(String[] args) throws InterruptedException {
    	final ActorSystem system = ActorSystem.create();
		final ActorMaterializer materializer = ActorMaterializer.create(system);
		
		writeSomeData(system, materializer);  // for testing
		createSupervisorStream(system, materializer);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				system.terminate();
			}
		});
		System.out.println("Ready");
		while(true) {
			Thread.sleep(3000);
		}
    }
    
    private static void createSupervisorStream(ActorSystem system, ActorMaterializer materializer) {
    	Source<EntitiesEvent, ?> detectionsSource = createSourceWithType(system, "creation", EntitiesEvent.Type.CREATE);
		Source<EntitiesEvent, ?> mergesSource = createSourceWithType(system, "merge", EntitiesEvent.Type.MERGE);
		Source<EntitiesEvent, ?> splitsSource = createSourceWithType(system, "split", EntitiesEvent.Type.SPLIT);
		Source<EntitiesEvent, ?> combinedSource = Source.fromGraph(GraphDSL.create(builder -> {
			UniformFanInShape<EntitiesEvent, EntitiesEvent> merger = builder.add(Merge.create(3));
			directToMerger(builder, detectionsSource, merger);
			directToMerger(builder, mergesSource, merger);
			directToMerger(builder, splitsSource, merger);
			return SourceShape.of(merger.out());
		}));
		
		EntitiesSupervisor supervisor = new EntitiesSupervisor(system, materializer);
		combinedSource
    		.to(Sink.foreach(supervisor::accept))
    		.run(materializer);
    }
    
    private static Source<EntitiesEvent, ?> createSourceWithType(ActorSystem system, String topic, EntitiesEvent.Type type) {
    	return KafkaSourceFactory.create(system, topic)
    			.via(Flow.fromFunction(r -> new EntitiesEvent(type, (GenericRecord) r.value())));
    }
    
    private static void directToMerger(Builder<NotUsed> builder, 
    		Source<EntitiesEvent, ?> source, UniformFanInShape<EntitiesEvent, ?> merger) {
    	builder.from(builder.add(source).out()).toFanIn(merger);
    }
    
    private static void writeSomeData(ActorSystem system, Materializer materializer) {
    	SchemaRegistryClient schemaRegistry = KafkaSourceFactory.schemaRegistry;
    	Schema.Parser parser = KafkaSourceFactory.parser;
    	
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers("localhost:9092");
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);
		
		String schemaString = "{\"type\": \"record\", "
						+ "\"name\": \"detectionEvent\", "
						+ "\"doc\": \"This is a schema for entity detection report event\", " 
						+ "\"fields\": [{ \"name\": \"sourceName\", "
										+ "\"type\": \"string\", "
										+ "\"doc\" : \"interface name\" }, " 
										+"{ \"name\": \"externalSystemID\", "
										+ "\"type\": \"string\", "
										+ "\"doc\":\"external system ID\"}]}";
		Schema creationSchema = parser.parse(schemaString);
		GenericRecord creationRecord = new GenericRecordBuilder(creationSchema)
				.set("sourceName", "source1")
				.set("externalSystemID", "id1")
				.build();
		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>("creation", creationRecord);
		
		Source.from(Arrays.asList(producerRecord))
			.to(sink)
			.run(materializer);
		Schema coordinateSchema = parser.getTypes().get("coordinate");
		GenericRecord coordinate = new GenericRecordBuilder(coordinateSchema)
				.set("lat", 4.5d)
				.set("long", 3.4d)
				.build();
		Schema basicAttributesSchema = parser.getTypes().get("basicEntityAttributes");
		GenericRecord basicAttributes = new GenericRecordBuilder(basicAttributesSchema)
				.set("coordinate", coordinate)
				.set("isNotTracked", false)
				.set("entityOffset", 50l)
				.build();
		Schema dataSchema = parser.getTypes().get("generalEntityAttributes");
		GenericRecord dataRecord = new GenericRecordBuilder(dataSchema)
				.set("basicAttributes", basicAttributes)
				.set("speed", 4.7)
				.set("elevation", 7.8)
				.set("course", 8.3)
				.set("nationality", new GenericData.EnumSymbol(parser.getTypes().get("nationality"), "USA"))
				.set("category", new GenericData.EnumSymbol(parser.getTypes().get("nationality"), "boat"))
				.set("pictureURL", "huh?")
				.set("height", 6.1)
				.set("nickname", "rerere")
				.set("externalSystemID", "id1")
				.build();
		ProducerRecord<String, Object> producerRecord2 = new ProducerRecord<String, Object>("source1", dataRecord);
		
		Source.from(Arrays.asList(producerRecord2))
			.to(sink)
			.run(materializer);
    }
}
