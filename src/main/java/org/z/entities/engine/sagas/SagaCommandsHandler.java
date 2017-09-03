package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.model.AggregateLifecycle;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.GenericEventMessage;
import org.z.entities.engine.EntitiesSupervisor;
import org.z.entities.engine.EntityManager;
import org.z.entities.engine.SonAccessor;
import org.z.entities.engine.SourceDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class SagaCommandsHandler {
    private EntitiesSupervisor entitiesSupervisor;
    private EventBus eventBus;

    public SagaCommandsHandler(EntitiesSupervisor entitiesSupervisor, EventBus eventBus) {
        this.entitiesSupervisor = entitiesSupervisor;
        this.eventBus = eventBus;
    }

    @CommandHandler
    public void stopEntities(MergeCommands.StopEntities command) {
        System.out.println("merge handler stopping entities, as requested by saga " + command.getSagaId());
        for (UUID entityId : command.getEntitiesToStop()) {
            System.out.println("merge handler stopping entity " + entityId);
            entitiesSupervisor.pauseEntity(entityId, command.getSagaId());
            // The EntityManagers will send the EntityStopped event
        }
    }

    @CommandHandler
    public void mergeEntities(MergeCommands.CreateMergedFamily command) {
        System.out.println("merge handler creating merged family, as requested by saga " + command.getSagaId());
        List<SourceDescriptor> sources = new ArrayList<>(command.getEntitiesToMerge().size());
        for (GenericRecord entity : command.getEntitiesToMerge()) {
            sources.addAll(getSources(entity));
        }
        entitiesSupervisor.createStream(null, sources, sources.get(0), UUID.randomUUID(), "CREATED", "deprecated");
        eventBus.publish(new GenericEventMessage<>(new MergeEvents.MergedFamilyCreated(command.getSagaId())));
    }

    @CommandHandler
    public void recoverEntities(MergeCommands.RecoverEntities command) {
        System.out.println("merge handler recovering entities, as requested by saga " + command.getSagaId());
        for (GenericRecord entity : command.getEntitiesToRecover()) {
            List<SourceDescriptor> sources = getSources(entity);
            UUID uuid = getEntityUuid(entity);
            entitiesSupervisor.createStream(null, sources, uuid, "NONE", "deprectaed");
        }
        eventBus.publish(new GenericEventMessage<>(new MergeEvents.DeletedEntitiesRecovered(command.getSagaId())));
    }

    @CommandHandler
    public void stopEntity(SplitCommands.StopMergedEntity command) {
        System.out.println("commands handler stopping merged entity, as requested by saga" + command.getSagaId());
        entitiesSupervisor.pauseEntity(command.getEntityId(), command.getSagaId());
    }

    @CommandHandler
    public void splitEntity(SplitCommands.SplitMergedEntity command) {
        System.out.println("commands handler splitting entity as requested by saga " + command.getSagaId());
        List<SourceDescriptor> sources = getSources(command.getMergedEntity());
        for (SourceDescriptor source : sources) {
            entitiesSupervisor.createStream(null, Arrays.asList(source), source.getSystemUUID(), "CREATED", "deprecated");
        }
        eventBus.publish(new GenericEventMessage<>(new SplitEvents.MergedEntitySplit(command.getSagaId())));
    }

    @CommandHandler
    public void recoverMergeEntity(SplitCommands.RecoverMergedEntity command) {
        System.out.println("commands handler recovering merged entity as requested by saga " + command.getSagaId());
        List<SourceDescriptor> sources = getSources(command.getMergedEntity());
        UUID uuid = getEntityUuid(command.getMergedEntity());
        entitiesSupervisor.createStream(null, sources, uuid, "NONE", "deprecated");
    }

    private UUID getEntityUuid(GenericRecord entity) {
        return UUID.fromString(entity.get("entityID").toString());
    }

    private List<SourceDescriptor> getSources(GenericRecord entity) {
        List<SourceDescriptor> sources = new ArrayList<>();
        List<GenericRecord> sons = (List<GenericRecord>) entity.get("sons");
        for (GenericRecord son : sons) {
            sources.add(SonAccessor.getSourceDescriptor(son));
        }
        return sources;
    }
}
