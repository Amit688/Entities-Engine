package org.z.entities.engine;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.z.entities.engine.streams.LastStatePublisher;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.SourceQueueWithComplete;

@RunWith(MockitoJUnitRunner.class)
public class EntitiesSupervisorTest extends EntitiesSupervisor {

	public EntitiesSupervisorTest() {
		super();
	}

	private String sourceName = "source0";
	@Mock
	private MailRoom mailRoom;
	@Mock
	private LastStatePublisher lastStatePublisher; 
	@Mock
	private Map<String, MailRoom> mailRooms;
	@Mock
	private KafkaComponentsFactory componentsFactory;
	@Mock
	private Materializer materializer;
	@InjectMocks
	private EntitiesSupervisor entitiesSupervisor;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> sink = Sink
				.ignore();
		when(componentsFactory.getSink()).thenReturn(sink);
		entitiesSupervisor.stopQueues = new HashMap<>();
	}

	/**
	 * 	The create entity method is getting a request to create a new entity
	 *  build a new flow from mail room to the process 
	 */
	@Test
	public void testCreateEntityQueueWasCreated() {
		entitiesSupervisor.stopQueues = new HashMap<>();
		ActorSystem system = ActorSystem.create();
		entitiesSupervisor.materializer = ActorMaterializer.create(system);
		when(mailRoom.getReportsQueue(anyString())).thenReturn(
				new LinkedBlockingQueue<GenericRecord>());
		when(mailRooms.get(anyString())).thenReturn(mailRoom);

		SourceDescriptor sourceDescriptor1 = new SourceDescriptor(sourceName,
				"externalSystemID0", 1, UUID.randomUUID());
		List<SourceDescriptor> sourceDescriptors = new ArrayList<SourceDescriptor>();
		sourceDescriptors.add(sourceDescriptor1);
		entitiesSupervisor.stopQueues = new HashMap<>();
		UUID uuid = UUID.randomUUID();
		entitiesSupervisor.createEntity(sourceDescriptors, uuid, "NONE", "");

		assertTrue(entitiesSupervisor.stopQueues.keySet().contains(uuid));
		assertNotNull(entitiesSupervisor.stopQueues.get(uuid));
	}
	/**
	 * 	The create entity method is getting a request to create a new entity
	 *  but the mail room of the interface is missing the message queue for that report id
	 *  the expected result is exception
	 */
	@Test(expected = RuntimeException.class)
	public void testCreateEntityQueueWasntCreated() {
		entitiesSupervisor.stopQueues = new HashMap<>();
		when(mailRoom.getReportsQueue(anyString())).thenReturn(null);

		SourceDescriptor sourceDescriptor1 = new SourceDescriptor(sourceName,
				"externalSystemID0", 1, UUID.randomUUID());
		List<SourceDescriptor> sourceDescriptors = new ArrayList<SourceDescriptor>();
		sourceDescriptors.add(sourceDescriptor1);
		entitiesSupervisor.createEntity(sourceDescriptors, UUID.randomUUID(),
				"NONE", "");
	}

	/**
	 * The stopQueues holds the stream for each entity (UUID)
	 * when the supervisor wants to stop entity need to send a STOPME message
	 * In case there is no such queue then throws an exception
	 */
	@Test(expected = RuntimeException.class)
	public void testStopEntityQueueIsMissing() {
		entitiesSupervisor.stopQueues = new HashMap<>();
		entitiesSupervisor.stopEntity(UUID.randomUUID(), UUID.randomUUID());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testStopEntityQueueExists() {
		UUID entityId = UUID.randomUUID();
		UUID sagaId = UUID.randomUUID();
		SourceQueueWithComplete<GenericRecord> stopSource = Mockito
				.mock(SourceQueueWithComplete.class);
		entitiesSupervisor.stopQueues = Mockito.mock(HashMap.class);
		when(entitiesSupervisor.stopQueues.get(any(UUID.class))).thenReturn(
				stopSource);

		entitiesSupervisor.stopEntity(entityId, sagaId);

		verify(stopSource, times(1)).offer(createStopMessage(sagaId));
	}
}
