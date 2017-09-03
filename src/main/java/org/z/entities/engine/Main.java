package org.z.entities.engine;

import java.io.IOException;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
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
import kamon.Kamon;
import org.axonframework.commandhandling.AsynchronousCommandBus;
import org.axonframework.config.Configuration;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.SagaConfiguration;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.z.entities.engine.sagas.*;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

	public static boolean testing = false;

	public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
		System.out.println("KAFKA_ADDRESS::::::::" + System.getenv("KAFKA_ADDRESS"));
		System.out.println("SCHEMA_REGISTRY_ADDRESS::::::::" + System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		System.out.println("SCHEMA_REGISTRY_IDENTITY::::::::" + System.getenv("SCHEMA_REGISTRY_IDENTITY"));
		System.out.println("SINGLE_SOURCE_PER_TOPIC::::::::" + System.getenv("SINGLE_SOURCE_PER_TOPIC"));
		System.out.println("SINGLE_SINK::::::::" + System.getenv("SINGLE_SINK"));
		System.out.println("KAMON_ENABLED::::::::" + System.getenv("KAMON_ENABLED"));
		System.out.println("CONF_IND::::::::" + System.getenv("CONF_IND"));
		System.out.println("INTERFACES_NAME::::::::" + System.getenv("INTERFACES_NAME"));

		boolean isKamonEnabled = Boolean.parseBoolean(System.getenv("KAMON_ENABLED"));
		final ActorSystem system; 
		final SchemaRegistryClient schemaRegistry;
		final KafkaComponentsFactory sourceFactory;

		if(!testing) {

			if(System.getenv("CONF_IND").equalsIgnoreCase("true")) {
				Config cfg = ConfigFactory.parseResources(Main.class, "/akka-streams.conf").resolve();
				system = ActorSystem.create("sys", cfg);	
			}
			else {
				system = ActorSystem.create();
			} 
			schemaRegistry = new CachedSchemaRegistryClient(System.getenv("SCHEMA_REGISTRY_ADDRESS"), Integer.parseInt(System.getenv("SCHEMA_REGISTRY_IDENTITY")));
			sourceFactory = new KafkaComponentsFactory(system, schemaRegistry,
					System.getenv("KAFKA_ADDRESS"), Boolean.parseBoolean(System.getenv("SINGLE_SOURCE_PER_TOPIC")),
					Boolean.parseBoolean(System.getenv("SINGLE_SINK")));

			if (isKamonEnabled) {
				Kamon.start();
			}

		}
		else {
			system = ActorSystem.create();
			schemaRegistry = new MockSchemaRegistryClient();
			registerSchemas(schemaRegistry);
//			sourceFactory = new KafkaComponentsFactory(system, schemaRegistry,
//					"192.168.0.51:9092", false,false);
			sourceFactory = new KafkaComponentsFactory(system, schemaRegistry,
					"localhost:9092", false,false);
		}

		final ActorMaterializer materializer = ActorMaterializer.create(system);
		
		Map<String,BackOffice> backOfficeMap = new HashMap<>();
		StringTokenizer st = new StringTokenizer(System.getenv("INTERFACES_NAME"), ",");
		while(st.hasMoreElements()){			
			String sourceName = st.nextToken();
			backOfficeMap.put(sourceName,createBackOfficeStream(materializer,sourceFactory,sourceName,system)); 
		}

		EntitiesSupervisor supervisor = new EntitiesSupervisor(materializer, sourceFactory,backOfficeMap);
		SagasManager sagasManager = new SagasManager();
		Configuration axonConfiguration = axonSetup(supervisor, sagasManager);

		createSupervisorStream(materializer, sourceFactory, supervisor);
		createSagasManagerStream(materializer, sourceFactory, sagasManager);
		//Simulator.writeSomeData(system, materializer, schemaRegistry,supervisor);
		if(testing) {
			Simulator.writeSomeDataForMailRoom(system, materializer, schemaRegistry,supervisor);
			Map<UUID, EntityManagerForMailRoom> entityManagers = supervisor.geAllEntityManager();
			Set<UUID> uuids = entityManagers.keySet();

			Simulator.writeMerge(system, materializer, schemaRegistry, uuids);
			Thread.sleep(2000);
			uuids = supervisor.geAllEntityManager().keySet();
			System.out.print("current uuids: ");
			uuids.forEach(uuid -> System.out.print(uuid + ", "));
			System.out.println();

			Simulator.writeSplit(system, materializer, schemaRegistry, uuids.iterator().next());
			Thread.sleep(2000);
			uuids = supervisor.geAllEntityManager().keySet();
			System.out.print("current uuids: ");
			uuids.forEach(uuid -> System.out.print(uuid + ", "));
			System.out.println();

			//backOfficeMap.get("source0").stopSourceQueueStream("id1_source_0");
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				system.terminate();
				if (isKamonEnabled) {
					Kamon.shutdown();
				}
				axonConfiguration.shutdown();
			}
		});

		System.out.println("Ready");
		while(true) {
			Thread.sleep(3000);
		}
	}

	private static BackOffice createBackOfficeStream(ActorMaterializer materializer,KafkaComponentsFactory sourceFactory,String sourceName, ActorSystem system) {

		Source<GenericRecord, ?> source =  sourceFactory.getSource(sourceName)
				.via(Flow.fromFunction(r -> (GenericRecord) r.value()));

		BackOffice backOffice = new BackOffice(sourceName,materializer,system);		
		source.to(Sink.foreach(backOffice::accept))
		.run(materializer);

		return backOffice;
	}


	private static void createSupervisorStream(ActorMaterializer materializer, KafkaComponentsFactory sourceFactory,
															 EntitiesSupervisor supervisor) {
		Source<EntitiesEvent, ?> detectionsSource = createSourceWithType(sourceFactory, "creation", EntitiesEvent.Type.CREATE);
//		Source<EntitiesEvent, ?> mergesSource = createSourceWithType(sourceFactory, "merge", EntitiesEvent.Type.MERGE);
//		Source<EntitiesEvent, ?> splitsSource = createSourceWithType(sourceFactory, "split", EntitiesEvent.Type.SPLIT);
//		Source<EntitiesEvent, ?> combinedSource = Source.fromGraph(GraphDSL.create(builder -> {
//			UniformFanInShape<EntitiesEvent, EntitiesEvent> merger = builder.add(Merge.create(3));
//			directToMerger(builder, detectionsSource, merger);
//			directToMerger(builder, mergesSource, merger);
//			directToMerger(builder, splitsSource, merger);
//			return SourceShape.of(merger.out());
//		}));

//		combinedSource
		detectionsSource
		.to(Sink.foreach(supervisor::accept))
		.run(materializer);
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

	private static Configuration axonSetup(EntitiesSupervisor entitiesSupervisor, SagasManager sagasManager) {
		Configuration configuration = DefaultConfigurer.defaultConfiguration()
				.configureCommandBus(c -> {
					AsynchronousCommandBus commandBus = new AsynchronousCommandBus();
					c.onShutdown(commandBus::shutdown);
					return commandBus;
				})
				.configureEmbeddedEventStore(c -> new InMemoryEventStorageEngine())
				.registerCommandHandler(c -> new SagaCommandsHandler(entitiesSupervisor, c.eventBus()))
				.registerCommandHandler(c -> sagasManager)
				.registerModule(SagaConfiguration.subscribingSagaManager(MergeSaga.class))
				.registerModule(SagaConfiguration.subscribingSagaManager(SplitSaga.class))
				.registerComponent(MergeValidationService.class, c -> new MergeValidationService())
				.registerComponent(SplitValidationService.class, c-> new SplitValidationService())
				.buildConfiguration();
		configuration.start();

		entitiesSupervisor.setEventBus(configuration.eventBus());
		sagasManager.setEventBus(configuration.eventBus());

		return configuration;
	}

	private static void createSagasManagerStream(ActorMaterializer materializer, KafkaComponentsFactory sourceFactory,
												 SagasManager sagasManager) {
		Source<EntitiesEvent, ?> mergesSource = createSourceWithType(sourceFactory, "merge", EntitiesEvent.Type.MERGE);
		Source<EntitiesEvent, ?> splitsSource = createSourceWithType(sourceFactory, "split", EntitiesEvent.Type.SPLIT);
		Source<EntitiesEvent, ?> combinedSource = Source.fromGraph(GraphDSL.create(builder -> {
			UniformFanInShape<EntitiesEvent, EntitiesEvent> merger = builder.add(Merge.create(2));
			directToMerger(builder, mergesSource, merger);
			directToMerger(builder, splitsSource, merger);
			return SourceShape.of(merger.out());
		}));

		combinedSource
				.to(Sink.foreach(sagasManager::accept))
				.run(materializer);
	}
}
