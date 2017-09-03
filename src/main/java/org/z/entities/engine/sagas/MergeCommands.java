package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class MergeCommands {
//    public static class CreateEntitiesAggregate {
//        @TargetAggregateIdentifier
//        private UUID aggregateId;
//        private UUID sagaId;
//        private Set<UUID> entities;
//
//        public CreateEntitiesAggregate(UUID aggregateId, UUID sagaId, Set<UUID> entities) {
//            this.aggregateId = aggregateId;
//            this.sagaId = sagaId;
//            this.entities = entities;
//        }
//
//        public UUID getAggregateId() {
//            return aggregateId;
//        }
//
//        public UUID getSagaId() {
//            return sagaId;
//        }
//
//        public Set<UUID> getEntities() {
//            return entities;
//        }
//    }

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
