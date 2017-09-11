package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.saga.SagaEventHandler;
import org.axonframework.eventhandling.saga.SagaLifecycle;
import org.axonframework.eventhandling.saga.StartSaga;

import javax.inject.Inject; 
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

public class SplitSaga {
    private UUID sagaId;
    private UUID mergedEntityId;
    private GenericRecord mergedEntity;

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
        commandGateway.send(new SplitCommands.StopMergedEntity(mergedEntityId, sagaId));
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void validateSplit(MergeEvents.EntityStopped event){ //SplitEvents.MergedEntityStopped event) {
        GenericRecord lastState = event.getLastState();
        if (validationService.validate(lastState)) {
            System.out.println("split saga " + sagaId + " found valid, proceeding");
            commandGateway.send(new SplitCommands.SplitMergedEntity(lastState, sagaId));
        } else {
            commandGateway.send(new SplitCommands.RecoverMergedEntity(lastState, sagaId));
        }
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void reportSuccess(SplitEvents.MergedEntitySplit event) {
        cleanup("Split operation successful");
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void reportFailure(SplitEvents.MergedEntityRecovered event) {
        cleanup("Split operation failed");
    }

    private void cleanup(String operationReport) {
        System.out.println("split saga " + sagaId + " finished with report: " + operationReport);
        commandGateway.send(new SagasManagerCommands.ReleaseEntities(new HashSet<>(Arrays.asList(mergedEntityId))));
        SagaLifecycle.end();
    }
}
