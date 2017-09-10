package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.saga.SagaEventHandler;
import org.axonframework.eventhandling.saga.SagaLifecycle;
import org.axonframework.eventhandling.saga.StartSaga;

import javax.inject.Inject;
import java.util.*;

public class MergeSaga {
    private UUID sagaId;
    private UUID entitiesAggregateId;
    private Collection<UUID> entitiesToMerge;
    private List<GenericRecord> entitiesLastState;

    @Inject
    private transient CommandGateway commandGateway;
    @Inject
    private transient MergeValidationService validationService;
    @Inject
    private transient Producer<String, String> messageProducer;

    @StartSaga
    @SagaEventHandler(associationProperty = "sagaId")
    public void stopEntities(MergeEvents.MergeRequested event) {
        initSaga(event.getSagaId(), event.getEntitiesToMerge());
        System.out.println("merge saga " + sagaId + " started");
        commandGateway.send(new MergeCommands.StopEntities(event.getEntitiesToMerge(), event.getSagaId()));
    }

    private void initSaga(UUID sagaId, Collection<UUID> entitiesToMerge) {
        this.sagaId = sagaId;
        this.entitiesAggregateId = UUID.randomUUID();
        this.entitiesToMerge = entitiesToMerge;
        this.entitiesLastState = new ArrayList<>(entitiesToMerge.size());
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void storeLastState(MergeEvents.EntityStopped event) {
        System.out.println("merge saga " + sagaId + " received event that target " + event.getLastState().get("entityID") + " stopped");
        // Sagas receive events serially, so no race condition
        entitiesLastState.add(event.getLastState());
        if (entitiesLastState.size() == entitiesToMerge.size()) {
            System.out.println("merge saga " + sagaId + " detected that all entities stopped");
            validateAndProceed();
        }
    }

    private void validateAndProceed() {
        if (validationService.validateMerge(entitiesLastState)) {
            System.out.println("merge saga " + sagaId + " found valid, proceeding");
            commandGateway.send(new MergeCommands.CreateMergedFamily(entitiesLastState, sagaId));
        } else {
            System.out.println("merge saga " + sagaId + " found invalid, restoring entities");
            commandGateway.send(new MergeCommands.RecoverEntities(entitiesLastState, sagaId));
        }
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void reportSuccess(MergeEvents.MergedFamilyCreated event) {
        cleanup("Merge operation successful");
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void reportFailure(MergeEvents.DeletedEntitiesRecovered event) {
        cleanup("Merge operation failed");
    }

    private void cleanup(String operationReport) {
        // Note- it's possible that the aggregate should delete itself, without waiting for a command from the saga
//        commandGateway.send(new MergeCommands.DeleteAggregate(entitiesAggregateId));
        System.out.println("merge saga " + sagaId + " finished with report: " + operationReport);
        commandGateway.send(new SagasManagerCommands.ReleaseEntities(entitiesToMerge));
//        messageProducer.send(new ProducerRecord<>("tacticalImageryOperationReport", operationReport));
        SagaLifecycle.end();
    }
}
