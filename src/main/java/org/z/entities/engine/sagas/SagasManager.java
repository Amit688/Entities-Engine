package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.GenericEventMessage;
import org.z.entities.engine.EntitiesEvent;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SagasManager implements Consumer<EntitiesEvent> {
    private Set<UUID> occupiedEntities;
    private EventBus eventBus;

    public SagasManager() {
        this.occupiedEntities = new HashSet<>();
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void accept(EntitiesEvent event) {
        try {
            GenericRecord data = event.getData();
            switch (event.getType()) {
                case MERGE:
                    mergeEntities(data);
                    break;
                case SPLIT:
                    splitEntity(data);
                    break;
                default:
                    System.out.println("received unknown event type: " + Objects.toString(event.getType()));
                    break;
            }
        } catch (RuntimeException e) {
            System.out.println("failed to process event of type " + Objects.toString(event.getType()));
            e.printStackTrace();
        }
    }

    public void mergeEntities(GenericRecord data) {
        System.out.println("Received merge event: " + data);
        List<Utf8> idsToMerge = (List<Utf8>) data.get("mergedEntitiesId");
        List<UUID> uuids = new ArrayList<>(idsToMerge.size());
        for (Utf8 id : idsToMerge) {
            uuids.add(UUID.fromString(id.toString()));
        }
        mergeEntities(uuids);
    }

    public UUID mergeEntities(Collection<UUID> entitiesToMerge) {
        occupiedEntities.addAll(entitiesToMerge);
        UUID sagaId = UUID.randomUUID();
        eventBus.publish(new GenericEventMessage(new MergeEvents.MergeRequested(sagaId, entitiesToMerge)));
        return sagaId;
    }

    private String uuidsToOutputString(Collection<UUID> uuids) {
        return uuids.stream()
                .map(uuid -> uuid.toString())
                .collect(Collectors.joining(", "));
    }

    public UUID splitEntity(GenericRecord data) {
        UUID uuid = UUID.fromString(data.get("splittedEntityID").toString());
        occupiedEntities.add(uuid);
        UUID sagaId = UUID.randomUUID();
        eventBus.publish(new GenericEventMessage(new SplitEvents.SplitRequested(sagaId, uuid)));
        return sagaId;
    }

    @CommandHandler
    public void releaseEntities(SagasManagerCommands.ReleaseEntities command) {
        occupiedEntities.removeAll(command.getEntitiesToRelease());
    }
}