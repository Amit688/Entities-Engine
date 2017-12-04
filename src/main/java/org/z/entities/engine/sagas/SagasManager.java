package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.GenericEventMessage;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.z.entities.engine.EntitiesEvent;
import org.z.entities.engine.Main;
import org.z.entities.engine.sagas.MergeEvents.MergeRequested;
import org.z.entities.engine.sagas.SplitEvents.SplitRequested;
import org.z.entities.engine.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SagasManager implements Consumer<EntitiesEvent> {
	private Set<UUID> occupiedEntities;
	private EventBus eventBus;
	private UUID uuidWaitingToRelease;
	private CountDownLatch latch; 

	final static public Logger logger = Logger.getLogger(SagasManager.class);
	static {
		Utils.setDebugLevel(logger);
	}   

	public SagasManager() {
		this.occupiedEntities = new ConcurrentHashSet<>(); 
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
				logger.error("received unknown event type: " + Objects.toString(event.getType()));
				break;
			}
		} catch (RuntimeException e) {
			logger.error("failed to process event of type " + Objects.toString(event.getType()));
			e.printStackTrace();
		}
	}

	public void mergeEntities(GenericRecord data) {
		logger.debug("Saga Manager received merge event: " + data);
		List<Utf8> idsToMerge = (List<Utf8>) data.get("mergedEntitiesId");
		List<UUID> uuids = new ArrayList<>(idsToMerge.size());
		for (Object id : idsToMerge) {
			UUID uuid = UUID.fromString(id.toString());
			uuids.add(uuid);
			checkAndWaitTillNotOccupied(uuid);
		}
		String metadata = (String) data.get("metadata");
		mergeEntities(uuids, metadata);
	}

	private UUID mergeEntities(Collection<UUID> entitiesToMerge, String metadata) {
		occupiedEntities.addAll(entitiesToMerge);
		UUID sagaId = UUID.randomUUID();
		eventBus.publish(new GenericEventMessage<MergeRequested>(new MergeEvents.MergeRequested(sagaId, entitiesToMerge, metadata)));
		return sagaId;
	}

	private String uuidsToOutputString(Collection<UUID> uuids) {
		return uuids.stream()
				.map(uuid -> uuid.toString())
				.collect(Collectors.joining(", "));
	}

	public UUID splitEntity(GenericRecord data) {
		logger.debug("Saga Manager received split event: " + data);
		UUID uuid = UUID.fromString(data.get("splittedEntityID").toString());
		checkAndWaitTillNotOccupied(uuid);
		occupiedEntities.add(uuid);
		UUID sagaId = UUID.randomUUID();
		String metadata = (String) data.get("metadata");
		eventBus.publish(new GenericEventMessage<SplitRequested>(new SplitEvents.SplitRequested(sagaId, uuid, metadata)));
		return sagaId;
	}

	@CommandHandler
	public void releaseEntities(SagasManagerCommands.ReleaseEntities command) {
		logger.debug("Sagas Manager recieved event to release entities");
		logger.debug("--->");
		command.getEntitiesToRelease().forEach(e -> logger.debug(e + ", ")); 
		if(uuidWaitingToRelease != null && command.getEntitiesToRelease().contains(uuidWaitingToRelease)) {
			latch.countDown();
		}
		occupiedEntities.removeAll(command.getEntitiesToRelease());
	}

	public Set<UUID> getOccupiedEntities() {
		return occupiedEntities;
	}

	private void checkAndWaitTillNotOccupied(UUID uuid) {
		if(occupiedEntities.contains(uuid)) {
			uuidWaitingToRelease = uuid;
			try {
				logger.debug("UUID "+uuid.toString()+" is being handled in another saga - waiting to release");
				latch = new CountDownLatch(1);
				latch.await();				
			} catch (InterruptedException e) { 
				e.printStackTrace();
			} 			    		
		}
		uuidWaitingToRelease = null; 
	}
}
