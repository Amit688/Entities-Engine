package org.z.entities.engine;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.MergeHub;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kamon.Kamon;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.axonframework.commandhandling.AsynchronousCommandBus;
import org.axonframework.config.Configuration;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.SagaConfiguration;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.z.entities.engine.sagas.MergeSaga;
import org.z.entities.engine.sagas.MergeValidationService;
import org.z.entities.engine.sagas.SagaCommandsHandler;
import org.z.entities.engine.sagas.SagasManager;
import org.z.entities.engine.sagas.SplitSaga;
import org.z.entities.engine.sagas.SplitValidationService;
import org.z.entities.engine.streams.EventBusPublisher;
import org.z.entities.engine.streams.LocalEntitiesOperator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

	public static boolean testing = true;

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
		final KafkaComponentsFactory componentsFactory;

		if(!testing) {
//			if(Objects.equals(System.getenv("CONF_IND"),"true")) {
//				Config cfg = ConfigFactory.parseResources(Main.class, "/akka-streams.conf").resolve();
//				system = ActorSystem.create("sys", cfg);
//			} else {
				system = ActorSystem.create();
//			}
			schemaRegistry = new CachedSchemaRegistryClient(System.getenv("SCHEMA_REGISTRY_ADDRESS"), Integer.parseInt(System.getenv("SCHEMA_REGISTRY_IDENTITY")));
			componentsFactory = new KafkaComponentsFactory(system, schemaRegistry,
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
			componentsFactory = new KafkaComponentsFactory(system, schemaRegistry,
					System.getenv("KAFKA_ADDRESS"), false,true);
		}

		final ActorMaterializer materializer = ActorMaterializer.create(system);
		
		Map<String,MailRoom> mailRooms = new HashMap<>();
		StringTokenizer st = new StringTokenizer(System.getenv("INTERFACES_NAME"), ",");
		while(st.hasMoreElements()){			
			String sourceName = st.nextToken();
			mailRooms.put(sourceName,createBackOfficeStream(materializer,componentsFactory,sourceName));
		}

		EventBusPublisher lastStatePublisher = new EventBusPublisher();
        EntitiesSupervisor supervisor = new EntitiesSupervisor(lastStatePublisher,
                mailRooms, componentsFactory, materializer);
        LocalEntitiesOperator entitiesOperator = new LocalEntitiesOperator(supervisor);
		SagasManager sagasManager = new SagasManager();
		Configuration axonConfiguration = axonSetup(lastStatePublisher, entitiesOperator, sagasManager);

		createSupervisorStream(materializer, supervisor, mailRooms);
		createSagasManagerStream(materializer, componentsFactory, sagasManager);
		//Simulator.writeSomeData(system, materializer, schemaRegistry,supervisor);
		if(testing) {
			simulateMergeAndSplit(system, materializer, schemaRegistry, supervisor, sagasManager, componentsFactory);
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

	private static MailRoom createBackOfficeStream(ActorMaterializer materializer, KafkaComponentsFactory sourceFactory, String sourceName) {
		MailRoom mailRoom = new MailRoom(sourceName, sourceFactory.getKafkaProducer());
		sourceFactory.getSource(sourceName)
                .via(Flow.fromFunction(r -> (GenericRecord) r.value()))
                .to(Sink.foreach(mailRoom::accept))
                .run(materializer);
		return mailRoom;
	}


	private static void createSupervisorStream(ActorMaterializer materializer,
                                               EntitiesSupervisor supervisor,
											   Map<String, MailRoom> mailRooms) {
		Sink<GenericRecord, ?> sink = MergeHub.of(GenericRecord.class)
				.via(Flow.fromFunction(record -> new EntitiesEvent(EntitiesEvent.Type.CREATE, record)))
				.to(Sink.foreach(supervisor::accept))
				.run(materializer);
		for (MailRoom mailRoom : mailRooms.values()) {
			SourceQueueWithComplete<GenericRecord> sourceQueue =
					Source.<GenericRecord>queue(100, OverflowStrategy.backpressure())
					.to(sink)
					.run(materializer);
			mailRoom.setCreationQueue(sourceQueue);
		}
	}

	private static Source<EntitiesEvent, ?> createSourceWithType(KafkaComponentsFactory sourceFactory, 
			String topic, EntitiesEvent.Type type) {
		return sourceFactory.getSource(topic)
				.via(Flow.fromFunction(r -> new EntitiesEvent(type, (GenericRecord) r.value())));
	}

	private static void directToMerger(Builder<?> builder,
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

	private static Configuration axonSetup(EventBusPublisher eventBusPublisher, LocalEntitiesOperator entitiesOperator,
                                           SagasManager sagasManager) {
		Configuration configuration = DefaultConfigurer.defaultConfiguration()
				.configureCommandBus(c -> {
					AsynchronousCommandBus commandBus = new AsynchronousCommandBus();
					c.onShutdown(commandBus::shutdown);
					return commandBus;
				})
				.configureEmbeddedEventStore(c -> new InMemoryEventStorageEngine())
				.registerCommandHandler(c -> new SagaCommandsHandler(entitiesOperator, c.eventBus()))
				.registerCommandHandler(c -> sagasManager)
				.registerModule(SagaConfiguration.subscribingSagaManager(MergeSaga.class))
				.registerModule(SagaConfiguration.subscribingSagaManager(SplitSaga.class))
				.registerComponent(MergeValidationService.class, c -> new MergeValidationService())
				.registerComponent(SplitValidationService.class, c-> new SplitValidationService())
				.buildConfiguration();
		configuration.start();

        eventBusPublisher.setEventBus(configuration.eventBus());
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

	private static void simulateMergeAndSplit(ActorSystem system, Materializer materializer, SchemaRegistryClient schemaRegistry,
                                              EntitiesSupervisor supervisor, SagasManager sagasManager, KafkaComponentsFactory componentsFactory)
            throws InterruptedException, IOException, RestClientException{
        Simulator.writeSomeDataForMailRoom(system, materializer, schemaRegistry, componentsFactory);
        printCurrentUuids(supervisor);
        printOccupiedUuids(sagasManager);

        Simulator.writeMerge(system, materializer, schemaRegistry, supervisor.getAllUuids());
        System.out.println("Started merge saga");
        printOccupiedUuids(sagasManager);
        Thread.sleep(2000);
        printCurrentUuids(supervisor);
        printOccupiedUuids(sagasManager);

        Simulator.writeSplit(system, materializer, schemaRegistry,
                supervisor.getAllUuids().iterator().next());
        System.out.println("Started split saga");
        printOccupiedUuids(sagasManager);
        Thread.sleep(2000);
        printCurrentUuids(supervisor);
        printOccupiedUuids(sagasManager);

        Simulator.writeSomeDataForMailRoom(system, materializer, schemaRegistry, componentsFactory);

//		Thread.sleep(2000);
//		Set<UUID> entities = supervisor.getAllUuids();
//		for(UUID uuid : entities) {
//			supervisor.stopEntity(uuid, null);
//		}

    }

    private static void printCurrentUuids(EntitiesSupervisor supervisor) {
        Set<UUID> uuids = supervisor.getAllUuids();
        System.out.print("current uuids: ");
        uuids.forEach(uuid -> System.out.print(uuid + ", "));
        System.out.println();
    }

    private static void printOccupiedUuids(SagasManager sagasManager) {
	    Set<UUID> uuids = sagasManager.getOccupiedEntities();
        System.out.print("occupied entities are: ");
        uuids.forEach(uuid -> System.out.print(uuid + ", "));
        System.out.println();
    }
}
