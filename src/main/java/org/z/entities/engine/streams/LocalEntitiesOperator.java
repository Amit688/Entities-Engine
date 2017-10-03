package org.z.entities.engine.streams;

import org.apache.avro.generic.GenericRecord;
import org.z.entities.engine.EntitiesSupervisor;
import org.z.entities.engine.SourceDescriptor;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class LocalEntitiesOperator implements EntitiesOperator {
    private EntitiesSupervisor entitiesSupervisor;

    public LocalEntitiesOperator(EntitiesSupervisor entitiesSupervisor) {
        this.entitiesSupervisor = entitiesSupervisor;
    }

    @Override
    public void createEntity(Collection<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange, String metadata) {
        entitiesSupervisor.createEntity(sourceDescriptors, uuid, stateChange, metadata);
    }

    @Override
    public void createEntity(Collection<SourceDescriptor> sourceDescriptors, Map<SourceDescriptor, GenericRecord> sons,
                             UUID uuid, String stateChange, String metadata) {
        entitiesSupervisor.createEntity(sourceDescriptors, sons, uuid, stateChange, metadata);
    }

    @Override
    public void stopEntity(UUID entityId, UUID sagaId) {
        entitiesSupervisor.stopEntity(entityId, sagaId);
    }

    @Override
    public void stopEntity(UUID entityId) {
        entitiesSupervisor.stopEntity(entityId, null);
    }
}
