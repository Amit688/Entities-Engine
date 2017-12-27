package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.GenericEventMessage;
import org.z.entities.engine.KafkaComponentsFactory;
import org.z.entities.engine.Main;
import org.z.entities.engine.SonAccessor;
import org.z.entities.engine.SourceDescriptor;
import org.z.entities.engine.streams.EntitiesOperator;
import org.z.entities.engine.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SagaCommandsHandler {
	private EntitiesOperator entitiesOperator;
	private EventBus eventBus;
	private KafkaComponentsFactory componentsFactory;
	final static public Logger logger = Logger.getLogger(Main.class);
	static {
		Utils.setDebugLevel(logger);
	}

	public SagaCommandsHandler(EntitiesOperator entitiesOperator, EventBus eventBus, KafkaComponentsFactory componentsFactory) {
		this.entitiesOperator = entitiesOperator;
		this.eventBus = eventBus;
		this.componentsFactory = componentsFactory;
	}

	@CommandHandler
	public void stopEntities(MergeCommands.StopEntities command) {
		logger.debug("merge handler stopping entities, as requested by saga " + command.getSagaId());
		for (UUID entityId : command.getEntitiesToStop()) {
			logger.debug("merge handler stopping entity " + entityId);
			entitiesOperator.stopEntity(entityId, command.getSagaId());
			// The EntityProcessors will send the EntityStopped event
		}
	}

	@CommandHandler
	public void mergeEntities(MergeCommands.CreateMergedFamily command) {
		UUID newUuid = UUID.randomUUID();
		logger.debug("merge handler creating merged family " + newUuid + ", as requested by saga " + command.getSagaId());
		List<SourceDescriptor> sources = new ArrayList<>(command.getEntitiesToMerge().size());
		Map<SourceDescriptor, GenericRecord> lastStates = new HashMap<>(sources.size());
		for(GenericRecord entity : command.getEntitiesToMerge()) {
			for(GenericRecord son : getSons(entity)) {
				
				int partition = componentsFactory.getPartitionByKey(son);
				SourceDescriptor sourceDescriptor = SonAccessor.getSourceDescriptor(son,partition);
				GenericRecord lastState = getSonState(son);
				sources.add(sourceDescriptor);
				lastStates.put(sourceDescriptor, lastState);
			}
		}
		logger.debug("sons of merged entity:");
		for(Map.Entry<SourceDescriptor, GenericRecord> entry : lastStates.entrySet()) {
			logger.debug(entry.getKey() + " ||| " + entry.getValue());
		}
		entitiesOperator.createEntity(sources, lastStates, newUuid, "CREATED", command.getMetadata());
		eventBus.publish(new GenericEventMessage<>(new MergeEvents.MergedFamilyCreated(command.getSagaId())));
	}

	@CommandHandler
	public void recoverEntities(MergeCommands.RecoverEntities command) {
		logger.debug("merge handler recovering entities, as requested by saga " + command.getSagaId());
		for (GenericRecord entity : command.getEntitiesToRecover()) {
			List<SourceDescriptor> sources = getSources(entity);
			UUID uuid = getEntityUuid(entity);
			entitiesOperator.createEntity(sources, uuid, "NONE", "");
		}
		eventBus.publish(new GenericEventMessage<>(new MergeEvents.DeletedEntitiesRecovered(command.getSagaId())));
	}

	@CommandHandler
	public void stopEntity(SplitCommands.StopMergedEntity command) {
		logger.debug("commands handler stopping merged entity, as requested by saga" + command.getSagaId());
		entitiesOperator.stopEntity(command.getEntityId(), command.getSagaId());
	}

	@CommandHandler
	public void splitEntity(SplitCommands.SplitMergedEntity command) {
		List<GenericRecord> sons = (List<GenericRecord>) command.getMergedEntity().get("sons");
		for (GenericRecord son : sons) {
			int partition = componentsFactory.getPartitionByKey(son);
			SourceDescriptor sourceDescriptor = SonAccessor.getSourceDescriptor(son,partition);
			GenericRecord state = (GenericRecord) son.get("entityAttributes");
			Map<SourceDescriptor, GenericRecord> stateMap = new HashMap<>(1);
			stateMap.put(sourceDescriptor, state);
			entitiesOperator.createEntity(Arrays.asList(sourceDescriptor), stateMap, sourceDescriptor.getSystemUUID(),
					"CREATED", command.getMetadata());
		}
		eventBus.publish(new GenericEventMessage<>(new SplitEvents.MergedEntitySplit(command.getSagaId())));
	}

	@CommandHandler
	public void recoverMergedEntity(SplitCommands.RecoverMergedEntity command) {
		logger.debug("commands handler recovering merged entity as requested by saga " + command.getSagaId());
		List<SourceDescriptor> sources = getSources(command.getMergedEntity());
		UUID uuid = getEntityUuid(command.getMergedEntity());
		entitiesOperator.createEntity(sources, uuid, "NONE", "");
		eventBus.publish(new GenericEventMessage<>(new SplitEvents.MergedEntityRecovered(command.getSagaId())));
	}

	private UUID getEntityUuid(GenericRecord entity) {
		return UUID.fromString(entity.get("entityID").toString());
	}

	private List<SourceDescriptor> getSources(GenericRecord entity) {

		List<GenericRecord> sons = (List<GenericRecord>) entity.get("sons");
		List<SourceDescriptor> sources = new ArrayList<>(sons.size());
		for (GenericRecord son : sons) {
			int partition = componentsFactory.getPartitionByKey(son);
			sources.add(SonAccessor.getSourceDescriptor(son,partition));
		}
		return sources;
	}

	private List<GenericRecord> getSons(GenericRecord entity) {
		return (List<GenericRecord>) entity.get("sons");
	}

	private GenericRecord getSonState(GenericRecord son) {
		return (GenericRecord) son.get("entityAttributes");
	}
}
