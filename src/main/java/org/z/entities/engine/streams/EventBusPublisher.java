package org.z.entities.engine.streams;

import org.apache.avro.generic.GenericRecord;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.GenericEventMessage;
import org.z.entities.engine.sagas.CommonEvents;

import java.util.UUID;

public class EventBusPublisher implements LastStatePublisher {

    private EventBus eventBus;

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void publish(GenericRecord lastState) {
       publish(lastState, null);
    }

    @Override
    public void publish(GenericRecord lastState, UUID sagaId) {
        eventBus.publish(new GenericEventMessage<>(new CommonEvents.EntityStopped(sagaId, lastState)));
    }
}
