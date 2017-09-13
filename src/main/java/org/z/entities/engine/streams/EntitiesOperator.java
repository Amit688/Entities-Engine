package org.z.entities.engine.streams;

import org.apache.avro.generic.GenericRecord;
import org.z.entities.engine.SourceDescriptor;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface EntitiesOperator {
    void createEntity(Collection<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange);

    void createEntity(Collection<SourceDescriptor> sourceDescriptors, Map<SourceDescriptor, GenericRecord> sons,
                      UUID uuid, String stateChange);

    void stopEntity(UUID entityId, UUID sagaId);

    void stopEntity(UUID entityId);
}
