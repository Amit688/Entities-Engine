package org.z.entities.engine.streams;

import org.apache.avro.generic.GenericRecord;

import java.util.UUID;

public interface LastStatePublisher {
    void publish(GenericRecord lastState);

    void publish(GenericRecord lastState, UUID sagaId);
}
