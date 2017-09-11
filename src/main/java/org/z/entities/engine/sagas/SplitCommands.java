package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord; 

import java.util.UUID;

public class SplitCommands {
    public static class StopMergedEntity {
        private UUID entityId;
        private UUID sagaId;

        public StopMergedEntity(UUID entityId, UUID sagaId) {
            this.entityId = entityId;
            this.sagaId = sagaId;
        }

        public UUID getEntityId() {
            return entityId;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }

    public static class SplitMergedEntity {
        private GenericRecord mergedEntity;
        private UUID sagaId;

        public SplitMergedEntity(GenericRecord mergedEntity, UUID sagaId) {
            this.mergedEntity = mergedEntity;
            this.sagaId = sagaId;
        }

        public GenericRecord getMergedEntity() {
            return mergedEntity;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }

    public static class RecoverMergedEntity {
        private GenericRecord mergedEntity;
        private UUID sagaId;

        public RecoverMergedEntity(GenericRecord mergedEntity, UUID sagaId) {
            this.mergedEntity = mergedEntity;
            this.sagaId = sagaId;
        }

        public GenericRecord getMergedEntity() {
            return mergedEntity;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }
}
