package org.z.entities.engine;

import static org.junit.Assert.*;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;  
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*; 
import akka.stream.javadsl.SourceQueueWithComplete;

@RunWith(MockitoJUnitRunner.class)
public class MailRoomTest {

	private MailRoom mailRoom;
	@Mock
	private SourceQueueWithComplete<GenericRecord> creationQueue;

	@Before
	public void setUp() throws Exception {
		mailRoom = new MailRoom("source0"); 
		when(creationQueue.offer(any())).thenAnswer(RETURNS_SMART_NULLS);
		mailRoom.setCreationQueue(creationQueue);
	}

	@Test
	public void testSingleMessageForNewId() {

		String externalSystemId = "externalSystemID0";
		try {
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId, ""));
		} catch (IOException | RestClientException e) {
			fail();
		}

		BlockingQueue<GenericRecord> queue = mailRoom
				.getReportsQueue(externalSystemId);
		assertEquals(queue.size(), 1);
		assertEquals(queue.poll().get("externalSystemID"), externalSystemId);
		assertEquals(queue.size(), 0);

		verify(creationQueue, times(1)).offer(any());
	}

	@Test
	public void testManyMessagesForNewId() {
		String externalSystemId = "externalSystemID0";
		try {
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId, "firstMessage"));
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId, "secondMessage"));
		} catch (IOException | RestClientException e) {
			fail();
		}

		BlockingQueue<GenericRecord> queue = mailRoom
				.getReportsQueue(externalSystemId);
		assertEquals(queue.size(), 2);
		assertEquals(queue.poll().get("metadata"), "firstMessage");
		assertEquals(queue.poll().get("metadata"), "secondMessage");
		assertEquals(queue.size(), 0);

		verify(creationQueue, times(1)).offer(any());
	}

	@Test
	public void testManyMessagesFor2Id() {
		String externalSystemId1 = "externalSystemID0";
		String externalSystemId2 = "externalSystemID1";
		try {
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId1, "firstMessage"));
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId1, "secondMessage"));
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId2, "firstMessage"));
			mailRoom.accept(TestUtils.getGenericRecord("source0",
					externalSystemId2, "secondMessage"));
		} catch (IOException | RestClientException e) {
			fail();
		}

		BlockingQueue<GenericRecord> queue = mailRoom
				.getReportsQueue(externalSystemId1);
		assertEquals(queue.size(), 2);
		assertEquals(queue.peek().get("externalSystemID"), externalSystemId1);
		assertEquals(queue.poll().get("metadata"), "firstMessage");
		assertEquals(queue.poll().get("metadata"), "secondMessage");
		assertEquals(queue.size(), 0);

		queue = mailRoom.getReportsQueue(externalSystemId2);
		assertEquals(queue.size(), 2);
		assertEquals(queue.peek().get("externalSystemID"), externalSystemId2);
		assertEquals(queue.poll().get("metadata"), "firstMessage");
		assertEquals(queue.poll().get("metadata"), "secondMessage");
		assertEquals(queue.size(), 0);

		verify(creationQueue, times(2)).offer(any());
	}
}
