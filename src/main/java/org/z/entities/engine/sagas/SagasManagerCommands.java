package org.z.entities.engine.sagas;

import java.util.Collection;
import java.util.Set;
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
    }
}
