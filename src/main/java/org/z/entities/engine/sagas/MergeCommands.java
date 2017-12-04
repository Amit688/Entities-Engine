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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((entitiesToStop == null) ? 0 : entitiesToStop.hashCode());
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
			StopEntities other = (StopEntities) obj;
			if (entitiesToStop == null) {
				if (other.entitiesToStop != null)
					return false;
			} else if (!entitiesToStop.equals(other.entitiesToStop))
				return false;
			if (sagaId == null) {
				if (other.sagaId != null)
					return false;
			} else if (!sagaId.equals(other.sagaId))
				return false;
			return true;
		}
    }

    public static class CreateMergedFamily {
        private Collection<GenericRecord> entitiesToMerge;
        private UUID sagaId;
        private String metadata;

        public CreateMergedFamily(Collection<GenericRecord> entitiesToMerge, UUID sagaId, String metadata) {
            this.entitiesToMerge = entitiesToMerge;
            this.sagaId = sagaId;
            this.metadata = metadata;
        }

        public Collection<GenericRecord> getEntitiesToMerge() {
            return entitiesToMerge;
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
			result = prime
					* result
					+ ((entitiesToMerge == null) ? 0 : entitiesToMerge
							.hashCode());
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
			CreateMergedFamily other = (CreateMergedFamily) obj;
			if (entitiesToMerge == null) {
				if (other.entitiesToMerge != null)
					return false;
			} else if (!entitiesToMerge.equals(other.entitiesToMerge))
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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((entitiesToRecover == null) ? 0 : entitiesToRecover
							.hashCode());
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
			RecoverEntities other = (RecoverEntities) obj;
			if (entitiesToRecover == null) {
				if (other.entitiesToRecover != null)
					return false;
			} else if (!entitiesToRecover.equals(other.entitiesToRecover))
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
