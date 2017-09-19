package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;

import java.util.UUID;

public class CommonEvents {
    public static class EntityStopped {
        private UUID sagaId;
        private GenericRecord lastState;

        public EntityStopped(UUID sagaId, GenericRecord lastState) {
            this.sagaId = sagaId;
            this.lastState = lastState;
        }

        public UUID getSagaId() {
            return sagaId;
        }

        public GenericRecord getLastState() {
            return lastState;
        }
    }
}
