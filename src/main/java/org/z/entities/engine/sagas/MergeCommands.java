package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;

import java.util.Collection;
import java.util.UUID;

public class MergeCommands {
    public static class StopEntities {
        private Collection<UUID> entitiesToStop;
        private UUID sagaId;

        public StopEntities(Collection<UUID> entitiesToStop, UUID sagaId) {
            this.entitiesToStop = entitiesToStop;
            this.sagaId = sagaId;
        }

        public Collection<UUID> getEntitiesToStop() {
            return entitiesToStop;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }

    public static class CreateMergedFamily {
        private Collection<GenericRecord> entitiesToMerge;
        private UUID sagaId;

        public CreateMergedFamily(Collection<GenericRecord> entitiesToMerge, UUID sagaId) {
            this.entitiesToMerge = entitiesToMerge;
            this.sagaId = sagaId;
        }

        public Collection<GenericRecord> getEntitiesToMerge() {
            return entitiesToMerge;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }

    public static class RecoverEntities {
        private Collection<GenericRecord> entitiesToRecover;
        private UUID sagaId;

        public RecoverEntities(Collection<GenericRecord> entitiesToRecover, UUID sagaId) {
            this.entitiesToRecover = entitiesToRecover;
            this.sagaId = sagaId;
        }

        public Collection<GenericRecord> getEntitiesToRecover() {
            return entitiesToRecover;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }
}
