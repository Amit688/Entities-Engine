package org.z.entities.engine.sagas;

import java.util.UUID;

public class SplitEvents {
    public static class SplitRequested {
        private UUID sagaId;
        private UUID mergedEntity;
        private String metadata;

        public SplitRequested(UUID sagaId, UUID mergedEntity, String metadata) {
            this.sagaId = sagaId;
            this.mergedEntity = mergedEntity;
            this.metadata = metadata;
        }

        public UUID getSagaId() {
            return sagaId;
        }

        public UUID getMergedEntity() {
            return mergedEntity;
        }

        public String getMetadata() {
            return metadata;
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
