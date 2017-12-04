package org.z.entities.engine;

import java.util.Arrays; 
import java.util.UUID;

import static org.mockito.Mockito.*; 
import static org.junit.Assert.*;

import org.axonframework.eventhandling.EventBus;  
import org.axonframework.eventhandling.EventMessage; 
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;   
import org.mockito.runners.MockitoJUnitRunner;
import org.z.entities.engine.sagas.SagasManager; 
import org.z.entities.engine.sagas.SagasManagerCommands.ReleaseEntities;
import org.z.entities.schema.MergeEvent;
import org.z.entities.schema.SplitEvent; 

@RunWith(MockitoJUnitRunner.class)
public class SagasManagerTest {
	
	private SagasManager sagasManager;
	@Mock
	private EventBus eventBus;
	
	@Before
	public void setUp() throws Exception {
		sagasManager = new SagasManager();
		sagasManager.setEventBus(eventBus);
	}

	/**
	 *  The saga manager is getting request for merge
	 *  create a new merge saga and check if the UUID is being handled by another open saga
	 *  if no - publish event on the event bus
	 *  expected result is to publish the request
	 */
	@Test
	public void testMergeActivityEntityIsNotOccupied() {
		sagasManager.accept(getMergeEvent(UUID.randomUUID(), UUID.randomUUID()));		 
		verify(eventBus, times(1)).publish(any(EventMessage.class));		
	}
	/**
	 *  The saga manager is getting request for merge
	 *  create a new merge saga and check if the UUID is being handled by another open saga
	 *  if yes - wait until the the saga is finished 
	 *  in that case - we create a new saga for UUID
	 *  then getting request to open another saga for the same UUID
	 *  the request is hanging until the UUID will be released by the thread
	 *  expected result is to publish the request of the second request only after the UUID is released
	 */	
	@Test 
	public void testMergeActivityEntityIsOccupied() {
		UUID uuid = UUID.randomUUID(); 
		sagasManager.accept(getMergeEvent(uuid, UUID.randomUUID()));	
		long startTime = System.currentTimeMillis();
		getThread(uuid).start();
		 
		sagasManager.accept(getMergeEvent(uuid, UUID.randomUUID()));		
		verify(eventBus, times(2)).publish(any(EventMessage.class));
		long duration = System.currentTimeMillis() - startTime; 
		assertTrue( duration > 1000 ? true : false );		
	}
	/**
	 *  The saga manager is getting request for split
	 *  create a new split saga and check if the UUID is being handled by another open saga
	 *  if no - publish event on the event bus
	 *  expected result is to publish the request
	 */
	@Test
	public void testSplitActivityEntityIsNotOccupied() {
		sagasManager.accept(getSplitEvent(UUID.randomUUID()));		
		verify(eventBus, times(1)).publish(any(EventMessage.class));		
	}
	/**
	 *  The saga manager is getting request for split
	 *  create a new split saga and check if the UUID is being handled by another open saga
	 *  if yes - wait until the the saga is finished 
	 *  in that case - we create a new saga for UUID
	 *  then getting request to open another saga for the same UUID
	 *  the request is hanging until the UUID will be released by the thread
	 *  expected result is to publish the request of the second request only after the UUID is released
	 */	
 	@Test
	public void testSplitActivityEntityIsOccupied() {
		UUID uuid = UUID.randomUUID(); 
		sagasManager.accept(getSplitEvent(uuid));
		long startTime = System.currentTimeMillis();
		getThread(uuid).start();
		
		sagasManager.accept(getSplitEvent(uuid));		
		verify(eventBus, times(2)).publish(any(EventMessage.class));
		long duration = System.currentTimeMillis() - startTime; 
		assertTrue( duration > 1000 ? true : false );	 
	}
	
	private EntitiesEvent getMergeEvent(UUID uuid1, UUID uuid2) {
		
		MergeEvent mergeEvent = MergeEvent
				.newBuilder()
				.setMergedEntitiesId(
						Arrays.asList(uuid1.toString(), uuid2.toString()))
						.setMetadata("").build();
		EntitiesEvent event = new EntitiesEvent(EntitiesEvent.Type.MERGE, mergeEvent);
		return event;
	}	
	
	private EntitiesEvent getSplitEvent(UUID uuid) {
		
		SplitEvent splitEvent = SplitEvent
				.newBuilder()
				.setSplittedEntityID(uuid.toString()) 
				.setMetadata("")
				.build();
		EntitiesEvent event = new EntitiesEvent(EntitiesEvent.Type.SPLIT, splitEvent);
		return event;
	}
	
	private Thread getThread(UUID uuid) {
		return new Thread() {			
			@Override
			public void run() { 
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) { 
				}
				ReleaseEntities command = new ReleaseEntities(Arrays.asList(uuid));
				sagasManager.releaseEntities(command);	 
			}
		};
	} 
}
