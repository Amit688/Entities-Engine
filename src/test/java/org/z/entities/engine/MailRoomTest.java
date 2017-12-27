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

//@RunWith(MockitoJUnitRunner.class)
public class MailRoomTest extends MailRoom {

	public MailRoomTest() {
		super(); 
	}

	private MailRoom mailRoom;
	//@Mock
	private SourceQueueWithComplete<GenericRecord> creationQueue;

//	@Before
	public void setUp() throws Exception {
		mailRoom = new MailRoom("source0"); 
		when(creationQueue.offer(any())).thenAnswer(RETURNS_SMART_NULLS);
		mailRoom.setCreationQueue(creationQueue);
	}

	/**
	 *  The mail room is getting messages from the kafka topic
	 *  and fill a message queue for each externalSystemId
	 *  only for the first time create the queue 
	 *  the expected result is to create 1 queue that will have 1 message
	 */
	//@Test
	public void testSingleMessageForNewId() {

		String externalSystemId = "externalSystemID0";
		String metadata = "junit";
		try {
			sendMessageToMailRoom("source0",externalSystemId,metadata); 
		} catch (IOException | RestClientException e) {
			fail();
		}

	//	BlockingQueue<GenericRecord> queue = mailRoom
	//			.getReportsQueue(externalSystemId);
	//	assertEquals(queue.size(), 1);
	//	assertEquals(queue.poll().get("externalSystemID"), externalSystemId);
	//	assertEquals(queue.size(), 0);

	//	try {
	//		verify(creationQueue, times(1)).offer(mailRoom.getGenericRecordForCreation(externalSystemId, metadata));
	//	} catch (IOException | RestClientException e) {
	//		fail();
	//	}
	}
	/**
	 *  The mail room is getting messages from the kafka topic
	 *  and fill a message queue for each externalSystemId
	 *  only for the first time create the queue 
	 *  the expected result is to create 1 queue that will have 2 messages
	 */
	/*@Test
	public void testManyMessagesForNewId() {
		String externalSystemId = "externalSystemID0";
		try {
			sendMessageToMailRoom("source0",externalSystemId,"firstMessage");
			sendMessageToMailRoom("source0",externalSystemId,"secondMessage"); 
		} catch (IOException | RestClientException e) {
			fail();
		}

		BlockingQueue<GenericRecord> queue = mailRoom
				.getReportsQueue(externalSystemId);
		assertEquals(queue.size(), 2);
		assertEquals(queue.poll().get("metadata"), "firstMessage");
		assertEquals(queue.poll().get("metadata"), "secondMessage");
		assertEquals(queue.size(), 0);

		try {
			verify(creationQueue, times(1)).offer(mailRoom.getGenericRecordForCreation(externalSystemId, "firstMessage"));
		} catch (IOException | RestClientException e) {
			fail();
		}
	}*/
	/**
	 *  The mail room is getting messages from the kafka topic
	 *  and fill a message queue for each externalSystemId
	 *  only for the first time create the queue 
	 *  In that test the mail room got 4 messages for 2 externalSystemId
	 *  the expected result is to create 2 queues and each queue will have 2 messages
	 */
	/*
	@Test
	public void testManyMessagesFor2Id() {
		String externalSystemId1 = "externalSystemID0";
		String externalSystemId2 = "externalSystemID1";
		try {
			sendMessageToMailRoom("source0",externalSystemId1,"firstMessage");
			sendMessageToMailRoom("source0",externalSystemId1,"secondMessage");
			sendMessageToMailRoom("source0",externalSystemId2,"firstMessage");
			sendMessageToMailRoom("source0",externalSystemId2,"secondMessage"); 
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

		verify(creationQueue, times(2)).offer(any(GenericRecord.class));
	}
	*/
	private void sendMessageToMailRoom(String sourceName,String externalSystemId,String metadata) throws IOException, RestClientException {
		mailRoom.accept(TestUtils.getGenericRecord(sourceName,externalSystemId, metadata));	
	} 
}
