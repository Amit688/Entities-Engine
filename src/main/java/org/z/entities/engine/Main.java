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

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
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
import org.z.entities.engine.utils.Utils;
import org.z.entities.schema.BasicEntityAttributes;
import org.z.entities.schema.DetectionEvent;
import org.z.entities.schema.EntityFamily;
import org.z.entities.schema.GeneralEntityAttributes;
import org.z.entities.schema.MergeEvent;
import org.z.entities.schema.SplitEvent;
import org.z.entities.schema.SystemEntity;

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
	final static public Logger logger = Logger.getLogger(Main.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
		logger.debug("KAFKA_ADDRESS::::::::" + System.getenv("KAFKA_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_ADDRESS::::::::" + System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_IDENTITY::::::::" + System.getenv("SCHEMA_REGISTRY_IDENTITY"));
		logger.debug("SINGLE_SOURCE_PER_TOPIC::::::::" + System.getenv("SINGLE_SOURCE_PER_TOPIC"));
		logger.debug("SINGLE_SINK::::::::" + System.getenv("SINGLE_SINK"));
		logger.debug("KAMON_ENABLED::::::::" + System.getenv("KAMON_ENABLED"));
		logger.debug("CONF_IND::::::::" + System.getenv("CONF_IND"));
		logger.debug("INTERFACES_NAME::::::::" + System.getenv("INTERFACES_NAME"));

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
		if(testing) {
			Simulator.writeSomeDataForMailRoom(system, materializer, schemaRegistry, componentsFactory);
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

		logger.debug("Ready");
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

		schemaRegistry.register("DetectionEvent",DetectionEvent.SCHEMA$);
		schemaRegistry.register("BasicEntityAttributes",BasicEntityAttributes.SCHEMA$);
		schemaRegistry.register("GeneralEntityAttributes",GeneralEntityAttributes.SCHEMA$);
		schemaRegistry.register("SystemEntity",SystemEntity.SCHEMA$);
		schemaRegistry.register("EntityFamily",EntityFamily.SCHEMA$);
		schemaRegistry.register("MergeEvent",MergeEvent.SCHEMA$);
		schemaRegistry.register("SplitEvent",SplitEvent.SCHEMA$); 
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
		logger.debug("Started merge saga");
		printOccupiedUuids(sagasManager);
		Thread.sleep(2000);
		printCurrentUuids(supervisor);
		printOccupiedUuids(sagasManager);

		//        Simulator.writeSplit(system, materializer, schemaRegistry,
		//                supervisor.getAllUuids().iterator().next());
		//        logger.debug("Started split saga");
		//        printOccupiedUuids(sagasManager);
		//        Thread.sleep(2000);
		//        printCurrentUuids(supervisor);
		//        printOccupiedUuids(sagasManager);
		//
		//        Simulator.writeSomeDataForMailRoom(system, materializer, schemaRegistry, componentsFactory);

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
		logger.debug("\n");
	}

	private static void printOccupiedUuids(SagasManager sagasManager) {
		Set<UUID> uuids = sagasManager.getOccupiedEntities();
		System.out.print("occupied entities are: ");
		uuids.forEach(uuid -> System.out.print(uuid + ", "));
		logger.debug("\n");
	}
}
