package org.z.entities.engine.sagas;

import java.util.UUID;

public class SplitEvents {
    public static class SplitRequested {
        private UUID sagaId;
        private UUID mergedEntity;

        public SplitRequested(UUID sagaId, UUID mergedEntity) {
            this.sagaId = sagaId;
            this.mergedEntity = mergedEntity;
        }

        public UUID getSagaId() {
            return sagaId;
        }

        public UUID getMergedEntity() {
            return mergedEntity;
        }
    }

    public static class MergedEntitySplit {
        private UUID sagaId;

        public MergedEntitySplit(UUID sagaId) {
            this.sagaId = sagaId;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }

    public static class MergedEntityRecovered {
        private UUID sagaId;

        public MergedEntityRecovered(UUID sagaId) {
            this.sagaId = sagaId;
        }

        public UUID getSagaId() {
            return sagaId;
        }
    }
}
