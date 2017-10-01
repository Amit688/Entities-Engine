package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.saga.SagaEventHandler;
import org.axonframework.eventhandling.saga.SagaLifecycle;
import org.axonframework.eventhandling.saga.StartSaga; 
import org.z.entities.engine.utils.Utils;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class MergeSaga {
    private UUID sagaId;
    private Collection<UUID> entitiesToMerge;
    private List<GenericRecord> entitiesLastState;
	final static public Logger logger = Logger.getLogger(MergeSaga.class);
	static {
		Utils.setDebugLevel(logger);
	}

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
        logger.debug("merge saga " + sagaId + " started");
        commandGateway.send(new MergeCommands.StopEntities(event.getEntitiesToMerge(), event.getSagaId()));
    }

    private void initSaga(UUID sagaId, Collection<UUID> entitiesToMerge) {
        this.sagaId = sagaId;
        this.entitiesToMerge = entitiesToMerge;
        this.entitiesLastState = new ArrayList<>(entitiesToMerge.size());
    }

    @SagaEventHandler(associationProperty = "sagaId")
    public void storeLastState(CommonEvents.EntityStopped event) {
    	logger.debug("merge saga " + sagaId + " received event that target " + event.getLastState().get("entityID") + " stopped");
        // Sagas receive events serially, so no race condition
        entitiesLastState.add(event.getLastState());
        if (entitiesLastState.size() == entitiesToMerge.size()) {
        	logger.debug("merge saga " + sagaId + " detected that all entities stopped");
            validateAndProceed();
        }
    }

    private void validateAndProceed() {
        if (validationService.validateMerge(entitiesLastState)) {
        	logger.debug("merge saga " + sagaId + " found valid, proceeding");
            commandGateway.send(new MergeCommands.CreateMergedFamily(entitiesLastState, sagaId));
        } else {
        	logger.debug("merge saga " + sagaId + " found invalid, restoring entities");
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
    	logger.debug("merge saga " + sagaId + " finished with report: " + operationReport);
        commandGateway.send(new SagasManagerCommands.ReleaseEntities(entitiesToMerge));
        SagaLifecycle.end();
    }
}
