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
        
        @Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((entityId == null) ? 0 : entityId.hashCode());
			result = prime * result
					+ ((sagaId == null) ? 0 : sagaId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StopMergedEntity other = (StopMergedEntity) obj;
			if (entityId == null) {
				if (other.entityId != null)
					return false;
			} else if (!entityId.equals(other.entityId))
				return false;
			if (sagaId == null) {
				if (other.sagaId != null)
					return false;
			} else if (!sagaId.equals(other.sagaId))
				return false;
			return true;
		}
    }

    public static class SplitMergedEntity {
        private GenericRecord mergedEntity;
        private UUID sagaId;
        private String metadata;

        public SplitMergedEntity(GenericRecord mergedEntity, UUID sagaId, String metadata) {
            this.mergedEntity = mergedEntity;
            this.sagaId = sagaId;
            this.metadata = metadata;
        }

        public GenericRecord getMergedEntity() {
            return mergedEntity;
        }

        public UUID getSagaId() {
            return sagaId;
        }

        public String getMetadata() {
            return metadata;
        }

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((mergedEntity == null) ? 0 : mergedEntity.hashCode());
			result = prime * result
					+ ((metadata == null) ? 0 : metadata.hashCode());
			result = prime * result
					+ ((sagaId == null) ? 0 : sagaId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SplitMergedEntity other = (SplitMergedEntity) obj;
			if (mergedEntity == null) {
				if (other.mergedEntity != null)
					return false;
			} else if (!mergedEntity.equals(other.mergedEntity))
				return false;
			if (metadata == null) {
				if (other.metadata != null)
					return false;
			} else if (!metadata.equals(other.metadata))
				return false;
			if (sagaId == null) {
				if (other.sagaId != null)
					return false;
			} else if (!sagaId.equals(other.sagaId))
				return false;
			return true;
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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((mergedEntity == null) ? 0 : mergedEntity.hashCode());
			result = prime * result
					+ ((sagaId == null) ? 0 : sagaId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RecoverMergedEntity other = (RecoverMergedEntity) obj;
			if (mergedEntity == null) {
				if (other.mergedEntity != null)
					return false;
			} else if (!mergedEntity.equals(other.mergedEntity))
				return false;
			if (sagaId == null) {
				if (other.sagaId != null)
					return false;
			} else if (!sagaId.equals(other.sagaId))
				return false;
			return true;
		}
    }
}
