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
import org.z.entities.engine.sagas.MergeCommands;
import org.z.entities.engine.sagas.MergeSaga;
import org.z.entities.engine.sagas.MergeValidationService;  
import org.z.entities.engine.sagas.SagasManagerCommands;

@RunWith(MockitoJUnitRunner.class)
public class MergeSagaTest {
	
	private List<UUID> entitiesToMerge;
	private UUID sagaId;
	private String metadata;
	private UUID uuid1;
	private UUID uuid2;
	
	@Mock
	private CommandGateway commandGateway;
	@Mock
	private MergeValidationService validationService;

	@InjectMocks
	private MergeSaga mergeSaga;

	@Before
	public void setUp() throws Exception {
		when(commandGateway.send(any())).thenAnswer(RETURNS_SMART_NULLS);
		sagaId = UUID.randomUUID();
		metadata = "junit";
		entitiesToMerge = new ArrayList<>();		
		uuid1 = UUID.randomUUID();
		uuid2 = UUID.randomUUID();
		entitiesToMerge.add(uuid1);
		entitiesToMerge.add(uuid2);		
	} 
	/**
	 *  Store last start is waiting to get the last state for all the entities
	 *  involved in the merge saga
	 *  in that case not all the states were arrived
	 *  Expected result - not publish the event	 
	 */
	@Test
	public void testMergeNotAllEntitiesLastStateWereArraived() {
		/*Stop entity*/
		MergeRequested event = new MergeRequested(sagaId, entitiesToMerge, metadata);
		mergeSaga.stopEntities(event);
		verify(commandGateway, times(1)).send(eq(new MergeCommands.StopEntities(entitiesToMerge, sagaId)));
		
		/*Getting last state for only one entity*/
		reset(commandGateway);
        GenericRecord family = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID", UUID.randomUUID().toString())
        .build();
		EntityStopped entityStopped = new EntityStopped(sagaId, family);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, never()).send(any());		
	}
	/**
	 *  Store last start is waiting to get the last state for all the entities
	 *  involved in the merge saga
	 *  in that case  all the states were arrived but the validation failed 
	 */
	@Test
	public void testMergeAllEntitiesLastStateWereArraivedValidateFailed() {
		/*Stop entity*/
		MergeRequested event = new MergeRequested(sagaId, entitiesToMerge, metadata);
		mergeSaga.stopEntities(event);
		verify(commandGateway, times(1)).send(eq(new MergeCommands.StopEntities(entitiesToMerge, sagaId)));
		
		/*Getting last state for one entity*/
		reset(commandGateway);
        GenericRecord family1 = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID", uuid1.toString())
        .build();
		EntityStopped entityStopped = new EntityStopped(sagaId, family1);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, never()).send(any());
		
		/*Getting last state for one entity - merge validation failed*/
		reset(commandGateway);
		when(validationService.validateMerge(anyCollectionOf(GenericRecord.class))).thenReturn(false);
		GenericRecord family2  = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID",uuid2.toString())
        .build();
		entityStopped = new EntityStopped(sagaId, family2);
		mergeSaga.storeLastState(entityStopped);
		List<GenericRecord> entitiesLastState = new ArrayList<>(2);
		entitiesLastState.add(family1);
		entitiesLastState.add(family2);		
		verify(commandGateway, times(1)).send(eq(new MergeCommands.RecoverEntities(entitiesLastState, sagaId)));	
		
		/*Report failure*/
		DeletedEntitiesRecovered deletedEntitiesRecovered = new DeletedEntitiesRecovered(sagaId);
		mergeSaga.reportFailure(deletedEntitiesRecovered);
		verify(commandGateway, times(1)).send(eq(new SagasManagerCommands.ReleaseEntities(entitiesToMerge))); 		
	}	
	
	/**
	 *  Store last start is waiting to get the last state for all the entities
	 *  involved in the merge saga
	 *  in that case  all the states were arrived but the validation passed 
	 */
	@Test
	public void testMergeAllEntitiesLastStateWereArraivedValidatePassed() {
		/*Stop entity*/
		MergeRequested event = new MergeRequested(sagaId, entitiesToMerge, metadata);
		mergeSaga.stopEntities(event);
		verify(commandGateway, times(1)).send(eq(new MergeCommands.StopEntities(entitiesToMerge, sagaId)));
		
		/*Getting last state for one entity*/
		reset(commandGateway);
        GenericRecord family1 = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID", uuid1.toString())
        .build();
		EntityStopped entityStopped = new EntityStopped(sagaId, family1);
		mergeSaga.storeLastState(entityStopped);
		verify(commandGateway, never()).send(any());
		
		/*Getting last state for one entity - merge validation failed*/
		reset(commandGateway);
		when(validationService.validateMerge(anyCollectionOf(GenericRecord.class))).thenReturn(true);
		GenericRecord family2  = new GenericRecordBuilder(TestUtils.getDummySchema())
        .set("entityID",uuid2.toString())
        .build();
		entityStopped = new EntityStopped(sagaId, family2);
		mergeSaga.storeLastState(entityStopped);
		List<GenericRecord> entitiesLastState = new ArrayList<>(2);
		entitiesLastState.add(family1);
		entitiesLastState.add(family2);		
		verify(commandGateway, times(1)).send(eq(new MergeCommands.CreateMergedFamily(entitiesLastState, sagaId, metadata))); 
		
		/*Report success*/
		reset(commandGateway);
		MergedFamilyCreated mergedFamilyCreated = new MergedFamilyCreated(sagaId); 
		mergeSaga.reportSuccess(mergedFamilyCreated); 
		verify(commandGateway, times(1)).send(any());		
	}  
}
