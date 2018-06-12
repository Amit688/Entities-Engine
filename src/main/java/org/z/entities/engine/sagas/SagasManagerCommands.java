package org.z.entities.engine.sagas;

import java.util.Collection;
import java.util.UUID;

public class SagasManagerCommands {
    public static class ReleaseEntities {
        private Collection<UUID> entitiesToRelease;

        public ReleaseEntities(Collection<UUID> entitiesToRelease) {
            this.entitiesToRelease = entitiesToRelease;
        }

        public Collection<UUID> getEntitiesToRelease() {
            return entitiesToRelease;
        }

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((entitiesToRelease == null) ? 0 : entitiesToRelease
							.hashCode());
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
			ReleaseEntities other = (ReleaseEntities) obj;
			if (entitiesToRelease == null) {
				if (other.entitiesToRelease != null)
					return false;
			} else if (!entitiesToRelease.equals(other.entitiesToRelease))
				return false;
			return true;
		}
    }
}
