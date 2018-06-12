package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.saga.EndSaga;
import org.axonframework.eventhandling.saga.SagaEventHandler;
import org.axonframework.eventhandling.saga.SagaLifecycle;
import org.axonframework.eventhandling.saga.StartSaga;
import org.z.entities.engine.utils.Utils;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

public class SplitSaga {
    private UUID sagaId;
    private UUID mergedEntityId;
    private String metadata;
	final static public Logger logger = Logger.getLogger(SplitSaga.class);
	static {
		Utils.setDebugLevel(logger);
	}   

    @Inject
    private transient CommandGateway commandGateway;
    @Inject
    private transient SplitValidationService validationService;

    public SplitSaga() {

    }

    @StartSaga
    @SagaEventHandler(associationProperty = "sagaId")
    public void stopEntity(SplitEvents.SplitRequested event) {
        sagaId = event.getSagaId();
        mergedEntityId = event.getMergedEntity();
        metadata = event.getMetadata();
        commandGateway.send(new SplitCommands.StopMergedEntity(mergedEntityId, sagaId));
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void validateSplit(CommonEvents.EntityStopped event){
        GenericRecord lastState = event.getLastState();
        if (validationService.validate(lastState)) {
             logger.debug("split saga " + sagaId + " found valid, proceeding");
            commandGateway.send(new SplitCommands.SplitMergedEntity(lastState, sagaId, metadata));
        } else {
            commandGateway.send(new SplitCommands.RecoverMergedEntity(lastState, sagaId));
        }
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "sagaId")
    public void reportSuccess(SplitEvents.MergedEntitySplit event) {
        cleanup("Split operation successful");
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "sagaId")    
    public void reportFailure(SplitEvents.MergedEntityRecovered event) {
        cleanup("Split operation failed");
    }

    private void cleanup(String operationReport) {
    	logger.debug("split saga " + sagaId + " finished with report: " + operationReport);
        commandGateway.send(new SagasManagerCommands.ReleaseEntities(new HashSet<>(Arrays.asList(mergedEntityId)))); 
    } 
    
    
}
