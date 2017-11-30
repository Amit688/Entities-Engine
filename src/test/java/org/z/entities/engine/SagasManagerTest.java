package org.z.entities.engine;

import java.util.Arrays;
import java.util.UUID;

import org.axonframework.eventhandling.EventBus;  
import org.axonframework.eventhandling.EventMessage; 
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;  
import org.mockito.runners.MockitoJUnitRunner;
import org.z.entities.engine.sagas.SagasManager; 
import org.z.entities.schema.MergeEvent;
import org.z.entities.schema.SplitEvent;

import static org.mockito.Mockito.*;

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

	@Test
	public void testMergeActivity() {
		MergeEvent mergeEvent = MergeEvent
				.newBuilder()
				.setMergedEntitiesId(
						Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
						.setMetadata("").build();
		EntitiesEvent event = new EntitiesEvent(EntitiesEvent.Type.MERGE, mergeEvent);
		
		sagasManager.accept(event);		 
		verify(eventBus, times(1)).publish(any(EventMessage.class));		
	}
	
	@Test
	public void testSplitActivity() {
		SplitEvent splitEvent = SplitEvent
				.newBuilder()
				.setSplittedEntityID(UUID.randomUUID().toString()) 
				.setMetadata("")
				.build();
		EntitiesEvent event = new EntitiesEvent(EntitiesEvent.Type.SPLIT, splitEvent);
		
		sagasManager.accept(event);		
		verify(eventBus, times(1)).publish(any(EventMessage.class));		
	}
}
