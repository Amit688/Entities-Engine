package org.z.entities.engine;

import static org.mockito.Mockito.*; 

import java.util.UUID;

import org.axonframework.commandhandling.gateway.CommandGateway; 
import org.junit.Before; 
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock; 
import org.mockito.runners.MockitoJUnitRunner; 
import org.z.entities.engine.sagas.CommonEvents.EntityStopped;
import org.z.entities.engine.sagas.SplitEvents.MergedEntityRecovered;
import org.z.entities.engine.sagas.SplitEvents.MergedEntitySplit;
import org.z.entities.engine.sagas.SplitEvents.SplitRequested;
import org.z.entities.engine.sagas.SplitSaga;
import org.z.entities.engine.sagas.SplitValidationService;

@RunWith(MockitoJUnitRunner.class)
public class SplitSagaTest {

	@Mock
	private CommandGateway commandGateway;
	@Mock
	private SplitValidationService validationService;

	@InjectMocks
	private SplitSaga splitSaga;

	@Before
	public void setUp() throws Exception {
		when(commandGateway.send(any())).thenAnswer(RETURNS_SMART_NULLS);
	}

	@Test
	public void testStopEntity() {
		SplitRequested event = new SplitRequested(UUID.randomUUID(),
				UUID.randomUUID(), "");
		splitSaga.stopEntity(event);
		verify(commandGateway, times(1)).send(any());
	}

	@Test
	public void testValidateSplitFailed() {
		when(validationService.validate(any())).thenReturn(false);
		EntityStopped event = new EntityStopped(UUID.randomUUID(), null);
		splitSaga.validateSplit(event);
		verify(commandGateway, times(1)).send(any());
	}

	@Test
	public void testValidateSplitPassed() {
		when(validationService.validate(any())).thenReturn(true);
		EntityStopped event = new EntityStopped(UUID.randomUUID(), null);
		splitSaga.validateSplit(event);
		verify(commandGateway, times(1)).send(any());
	}

	@Test
	public void testReportSuccess() {

		MergedEntitySplit event = new MergedEntitySplit(UUID.randomUUID()); 
		splitSaga.reportSuccess(event); 
		verify(commandGateway, times(1)).send(any());
	}

	@Test
	public void testReportFailure() {
		MergedEntityRecovered event = new MergedEntityRecovered(
				UUID.randomUUID()); 
		splitSaga.reportFailure(event); 
		verify(commandGateway, times(1)).send(any());
	}

}
