package org.z.entities.engine;

import static org.mockito.Mockito.*; 
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException; 
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.axonframework.commandhandling.gateway.CommandGateway; 
import org.junit.Before; 
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock; 
import org.mockito.runners.MockitoJUnitRunner; 
import org.z.entities.engine.sagas.CommonEvents.EntityStopped;
import org.z.entities.engine.sagas.SplitEvents.MergedEntityRecovered; 
import org.z.entities.engine.sagas.SplitEvents.SplitRequested;
import org.z.entities.engine.sagas.SagasManagerCommands;
import org.z.entities.engine.sagas.SplitCommands;
import org.z.entities.engine.sagas.SplitSaga;
import org.z.entities.engine.sagas.SplitValidationService;

@RunWith(MockitoJUnitRunner.class)
public class SplitSagaTest {
	
	private UUID mergedEntityId;
	private UUID sagaId;
	private String metadata;

	@Mock
	private CommandGateway commandGateway;
	@Mock
	private SplitValidationService validationService;

	@InjectMocks
	private SplitSaga splitSaga;

	@Before
	public void setUp() throws Exception { 
		mergedEntityId = UUID.randomUUID();
		sagaId = UUID.randomUUID();
		metadata = "junit";
	}

	/**
	 *  Describe the split saga flow -
	 *  Stop entity
	 *  Getting entity last state
	 *  Validate that merge is allowed
	 *  If no - recover the entity (that was stopped)
	 *  Send the saga status
	 * @throws RestClientException 
	 * @throws IOException 
	 */
	@Test
	public void testSplitValidationFailed() throws IOException, RestClientException {
		/*Stop entity*/		 
		SplitRequested splitRequested = new SplitRequested(sagaId,mergedEntityId, metadata);
		splitSaga.stopEntity(splitRequested);
		verify(commandGateway, times(1)).send(eq(new SplitCommands.StopMergedEntity(mergedEntityId, sagaId)));
		
		/*Validate the split according to the input last state*/
		reset(commandGateway);
		when(validationService.validate(any())).thenReturn(false);
		GenericRecord lastState = TestUtils.getGenericRecord("source0", "externalSystemID0",metadata);		
		
		EntityStopped entityStopped = new EntityStopped(sagaId, lastState);
		splitSaga.validateSplit(entityStopped); 
		verify(commandGateway, times(1)).send(eq(new SplitCommands.RecoverMergedEntity(lastState, sagaId))); 
		
		/*Report that split request  was rejected*/
		reset(commandGateway);
		MergedEntityRecovered mergedEntityRecovered = new MergedEntityRecovered(mergedEntityId);				
		splitSaga.reportFailure(mergedEntityRecovered); 
		verify(commandGateway, times(1)).send(any(SagasManagerCommands.ReleaseEntities.class)); 
	}
	
	/**
	 *  Describe the split saga flow -
	 *  Stop entity
	 *  Getting entity last state
	 *  Validate that merge is allowed
	 *  If yes - recover the entity (that was stopped)
	 *  Send the saga status
	 * @throws RestClientException 
	 * @throws IOException 
	 */
	@Test
	public void testSplitValidationPassed() throws IOException, RestClientException {
		/*Stop entity*/		 
		SplitRequested splitRequested = new SplitRequested(sagaId,mergedEntityId, metadata);
		splitSaga.stopEntity(splitRequested);
		verify(commandGateway, times(1)).send(eq(new SplitCommands.StopMergedEntity(mergedEntityId, sagaId)));
		
		/*Validate the split according to the input last state*/
		reset(commandGateway);
		when(validationService.validate(any())).thenReturn(true);
		GenericRecord lastState = TestUtils.getGenericRecord("source0", "externalSystemID0",metadata);		
		
		EntityStopped entityStopped = new EntityStopped(sagaId, lastState);
		splitSaga.validateSplit(entityStopped); 
		verify(commandGateway, times(1)).send(eq(new SplitCommands.SplitMergedEntity(lastState, sagaId,metadata))); 
		
		/*Report that split request passed*/
		reset(commandGateway);
		MergedEntityRecovered mergedEntityRecovered = new MergedEntityRecovered(mergedEntityId);				
		splitSaga.reportFailure(mergedEntityRecovered); 
		verify(commandGateway, times(1)).send(any(SagasManagerCommands.ReleaseEntities.class)); 
	} 
}
