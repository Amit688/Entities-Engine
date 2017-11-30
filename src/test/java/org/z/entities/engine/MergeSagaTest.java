package org.z.entities.engine;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.junit.Before; 
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.z.entities.engine.sagas.CommonEvents.EntityStopped;
import org.z.entities.engine.sagas.MergeEvents.DeletedEntitiesRecovered;
import org.z.entities.engine.sagas.MergeEvents.MergeRequested;
import org.z.entities.engine.sagas.MergeEvents.MergedFamilyCreated; 
import org.z.entities.engine.sagas.MergeSaga;
import org.z.entities.engine.sagas.MergeValidationService;  

@RunWith(MockitoJUnitRunner.class)
public class MergeSagaTest {
	
	private List<UUID> entitiesToMerge;
	@Mock
	private CommandGateway commandGateway;
	@Mock
	private MergeValidationService validationService;

	@InjectMocks
	private MergeSaga mergeSaga;

	@Before
	public void setUp() throws Exception {
		when(commandGateway.send(any())).thenAnswer(RETURNS_SMART_NULLS);
		entitiesToMerge = new ArrayList<>();
		entitiesToMerge.add(UUID.randomUUID());
		entitiesToMerge.add(UUID.randomUUID());
	}
 
	@Test
	public void testStopEntities() { 
		MergeRequested event = new MergeRequested(UUID.randomUUID(), entitiesToMerge, "");
		mergeSaga.stopEntities(event);
		verify(commandGateway, times(1)).send(any());
	}

	@Test
	public void testStoreLastStateNotLastEntity() {
		
		MergeRequested mergeRequested = new MergeRequested(UUID.randomUUID(), entitiesToMerge, "");
		mergeSaga.stopEntities(mergeRequested);		
		
        GenericRecord family = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID", UUID.randomUUID().toString())
        .build();
		EntityStopped entityStopped = new EntityStopped(UUID.randomUUID(), family);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, times(1)).send(any());
	}
	
	@Test
	public void testStoreLastStateLastEntityValidationPassed() {
		
		MergeRequested mergeRequested = new MergeRequested(UUID.randomUUID(), entitiesToMerge, "");
		mergeSaga.stopEntities(mergeRequested);		
		
        GenericRecord family = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID", UUID.randomUUID().toString())
        .build();
		EntityStopped entityStopped = new EntityStopped(UUID.randomUUID(), family);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, times(1)).send(any());
		
		entityStopped = new EntityStopped(UUID.randomUUID(), family);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, times(2)).send(any());
	}
	
	
	@Test
	@SuppressWarnings("unchecked")
	public void testStoreLastStateLastEntityValidationFailed() {		
		when(validationService.validateMerge(anyCollection())).thenReturn(false);
		
		MergeRequested mergeRequested = new MergeRequested(UUID.randomUUID(), entitiesToMerge, "");
		mergeSaga.stopEntities(mergeRequested);		
		
        GenericRecord family = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID", UUID.randomUUID().toString())
        .build();
		EntityStopped entityStopped = new EntityStopped(UUID.randomUUID(), family);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, times(1)).send(any());
		
		entityStopped = new EntityStopped(UUID.randomUUID(), family);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, times(2)).send(any());
	}

	@Test
	public void testReportSuccess() {
		MergedFamilyCreated event = new MergedFamilyCreated(UUID.randomUUID()); 
		mergeSaga.reportSuccess(event); 
		verify(commandGateway, times(1)).send(any());
	}

	@Test
	public void testReportFailure() {
		DeletedEntitiesRecovered event = new DeletedEntitiesRecovered(UUID.randomUUID());
		mergeSaga.reportFailure(event);
		verify(commandGateway, times(1)).send(any());
	}

}
