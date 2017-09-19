package org.z.entities.engine.sagas;

import java.util.Collection;
import java.util.UUID;

public class MergeEvents {
    public static class MergeRequested {
        private UUID sagaId;
        private Collection<UUID> entitiesToMerge;

        public MergeRequested(UUID sagaId, Collection<UUID> entitiesToMerge) {
            this.sagaId = sagaId;
            this.entitiesToMerge = entitiesToMerge;
        }

        public UUID getSagaId() {
            return sagaId;
        }

        public Collection<UUID> getEntitiesToMerge() {
            return entitiesToMerge;
        }
    }

    public static class MergedFamilyCreated {
        private UUID sagaId;

        public MergedFamilyCreated(UUID sagaId) {
            this.sagaId = sagaId;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }

    public static class DeletedEntitiesRecovered {
        private UUID sagaId;

        public DeletedEntitiesRecovered(UUID sagaId) {
            this.sagaId = sagaId;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }
}
