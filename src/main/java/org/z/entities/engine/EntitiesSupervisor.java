package org.z.entities.engine;

import akka.NotUsed;
import akka.stream.ClosedShape;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.MergePreferred;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.z.entities.engine.streams.EntityProcessor;
import org.z.entities.engine.streams.EntityProcessorStage;
import org.z.entities.engine.streams.InterfaceSource;
import org.z.entities.engine.streams.LastStatePublisher;
import org.z.entities.engine.streams.StreamCompleter;
import org.z.entities.engine.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class EntitiesSupervisor implements Consumer<EntitiesEvent> {
    private LastStatePublisher lastStatePublisher;
  //  private Map<String, MailRoom> mailRooms;
    private KafkaComponentsFactory componentsFactory;
    protected Materializer materializer;
    protected Map<UUID, SourceQueueWithComplete<ConsumerRecord<Object, Object>>> stopQueues;
    
    final static public Logger logger = Logger.getLogger(EntitiesSupervisor.class);
	static {
		Utils.setDebugLevel(logger);
	}

    public EntitiesSupervisor(LastStatePublisher lastStatePublisher, Map<String, MailRoom> mailRooms,
                              KafkaComponentsFactory componentsFactory, Materializer materializer) {
        this.lastStatePublisher = lastStatePublisher;
    //    this.mailRooms = mailRooms;
        this.componentsFactory = componentsFactory;
        this.materializer = materializer;
        this.stopQueues = new HashMap<>();
    }

    public EntitiesSupervisor() {
		// TODO Auto-generated constructor stub
	}

	@Override
    public void accept(EntitiesEvent entitiesEvent) {
        try {
    		GenericRecord data = entitiesEvent.getData();
	    	switch (entitiesEvent.getType()) {
			case CREATE:
                createEntity(data);
				break;
            case STOP:
                stopEntity(data);
                break;
			default:
				logger.debug("received unknown event type: " + Objects.toString(entitiesEvent.getType()));
				break;
	    	}
    	} catch (RuntimeException e) {
    		logger.error("failed to process event of type " + Objects.toString(entitiesEvent.getType()));
    		e.printStackTrace();
    	}
    }

    private void createEntity(GenericRecord data) {
     	logger.debug("DATA IS: \n" + data.toString());
		SourceDescriptor sourceDescriptor = new SourceDescriptor(
				data.get("sourceName").toString(), // Is actually a org.apache.avro.util.Utf8
				data.get("externalSystemID").toString(),
				(long)data.get("dataOffset"),
				//componentsFactory.getPartitionByKey(data.get("sourceName").toString(), data.get("externalSystemID").toString()),
				(int)data.get("partition"),
				UUID.randomUUID());
		logger.debug("creating entity manager stream for source " + sourceDescriptor);
		String metadata = (String) data.get("metadata");
        createEntity(Arrays.asList(sourceDescriptor), sourceDescriptor.getSystemUUID(), "NONE", metadata);
    }

    /**
     * Create a new entity, with no initial state (bad idea if there is more than one source).
     * @param sourceDescriptors
     * @param uuid
     * @param stateChange
     */
    public void createEntity(Collection<SourceDescriptor> sourceDescriptors, UUID uuid, String stateChange,
                             String metadata) {
        Map<SourceDescriptor, GenericRecord> sons = new HashMap<>(sourceDescriptors.size());
        sourceDescriptors.forEach(sourceDescriptor -> sons.put(sourceDescriptor, null));
        createEntity(sourceDescriptors, sons, uuid, stateChange, metadata, false);
    }

    public void createEntity(Collection<SourceDescriptor> sourceDescriptors, Map<SourceDescriptor, GenericRecord> sons,
                             UUID uuid, String stateChange, String metadata) {
        createEntity(sourceDescriptors, sons, uuid, stateChange, metadata, true);
    }

    private void createEntity(Collection<SourceDescriptor> sourceDescriptors, Map<SourceDescriptor, GenericRecord> sons,
                              UUID uuid, String stateChange, String metadata, boolean sendInitialState) {
    	logger.debug("EntitySupervisor creating new entity " + uuid);
    	logger.debug("Send initial state " + sendInitialState);
        SourceDescriptor preferredSource = sourceDescriptors.iterator().next();
        EntityProcessor entityProcessor = new EntityProcessor(uuid, sons,
                preferredSource, stateChange, metadata);
        EntityProcessorStage entityProcessorStage = new EntityProcessorStage(entityProcessor, sendInitialState);
        StreamCompleter streamCompleter = new StreamCompleter(this, entityProcessor);
        SourceQueueWithComplete<ConsumerRecord<Object, Object>> stopQueue =
                createStream(sourceDescriptors, entityProcessorStage, streamCompleter);
        stopQueues.put(uuid, stopQueue);
    }

    private SourceQueueWithComplete<ConsumerRecord<Object, Object>> createStream(
            Collection<SourceDescriptor> sourceDescriptors,
            EntityProcessorStage entityProcessorStage,
            StreamCompleter streamCompleter) {
        List<Source<ConsumerRecord<Object, Object>, ?>> sources = createSources(sourceDescriptors);
        Source<ConsumerRecord<Object, Object>, SourceQueueWithComplete<ConsumerRecord<Object, Object>>> stopSource =
                Source.queue(1, OverflowStrategy.backpressure());
        Flow<ConsumerRecord<Object, Object>, ConsumerRecord<Object, Object>, ?> completerFlow = Flow.fromGraph(streamCompleter);
        Flow<ConsumerRecord<Object, Object>, ProducerRecord<Object, Object>, ?> processorFlow = Flow.fromGraph(entityProcessorStage);

        return createAndRunGraph(sources, stopSource, completerFlow, processorFlow);
    }

    private List<Source<ConsumerRecord<Object, Object>, ?>> createSources(Collection<SourceDescriptor> sourceDescriptors) {
        List<Source<ConsumerRecord<Object, Object>, ?>> sources = new ArrayList<>(sourceDescriptors.size());
        for (SourceDescriptor sourceDescriptor : sourceDescriptors) {
            //MailRoom mailRoom = mailRooms.get(sourceDescriptor.getSensorId()); 
            //BlockingQueue<GenericRecord> queue = mailRoom.getReportsQueue(sourceDescriptor.getReportsId());
            //if(queue == null) {
            //	throw new RuntimeException("Message queue is missing");
            //} 
            //Source<GenericRecord, ?> source = Source.fromGraph(new InterfaceSource(
            //       queue, sourceDescriptor.getSensorId() + "-|-" + sourceDescriptor.getReportsId()));
            Source<ConsumerRecord<Object, Object>, ?> source = componentsFactory.getSource(sourceDescriptor);
            sources.add(source);
        }
        return sources;
    }

    private SourceQueueWithComplete<ConsumerRecord<Object, Object>> createAndRunGraph(List<Source<ConsumerRecord<Object, Object>, ?>> sources,
                                   Source<ConsumerRecord<Object, Object>, SourceQueueWithComplete<ConsumerRecord<Object, Object>>> stopSource,
                                   Flow<ConsumerRecord<Object, Object>, ConsumerRecord<Object, Object>, ?> completerFlow,
                                   Flow<ConsumerRecord<Object, Object>, ProducerRecord<Object, Object>, ?> processorFlow) {
        final RunnableGraph<SourceQueueWithComplete<ConsumerRecord<Object, Object>>> result = RunnableGraph.fromGraph(GraphDSL.create(
                componentsFactory.getSink(),
                stopSource,
                Keep.right(),
                (builder, out, stop) -> {
                    final akka.stream.scaladsl.MergePreferred.MergePreferredShape<ConsumerRecord<Object, Object>> merge =
                            builder.add(MergePreferred.create(sources.size())); 
                    for (Source<ConsumerRecord<Object, Object>, ?> source : sources) {
                        builder.from(builder.add(source))
                                .toFanIn(merge);
                    }
                    builder.from(stop)
                            .toInlet(merge.preferred());
                    builder.from(merge.out())
                            .via(builder.add(completerFlow))
                            .via(builder.add(processorFlow))
                            .to(out);
                    return ClosedShape.getInstance();
                }));
        return result.run(materializer);
    }

    public void stopEntity(GenericRecord data) {
        UUID entityId = UUID.fromString(data.get("uuid").toString());
        stopEntity(entityId, null);
    }

    public void stopEntity(UUID entityId, UUID sagaId) {
        SourceQueueWithComplete<ConsumerRecord<Object, Object>> stopSource = stopQueues.get(entityId);
        if (stopSource != null) {
        	logger.debug("EntitiesSupervisor stopping entity " + entityId);
            stopSource.offer(createStopMessage(sagaId));
        } else {
        	logger.error("Tried to stop non-existent entity " + entityId);
        	throw new RuntimeException("Tried to stop non-existent entity " + entityId);
        }
    }

    protected ConsumerRecord<Object, Object> createStopMessage(UUID sagaId) {
        Schema schema = SchemaBuilder.builder().record("stopMeMessage").fields()
                .optionalString("sagaId")
                .endRecord();

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        if (sagaId != null) {
            builder.set("sagaId", sagaId.toString());
        }
        return new ConsumerRecord<Object, Object>("topic",0,0,"dummyKey",builder.build()); 
    }

    public void notifyOfStreamCompletion(UUID entityId, GenericRecord lastState, UUID sagaId) {
        logger.debug("EntitiesSupervisor notified of stream completion " + entityId);
        stopQueues.remove(entityId);
        lastStatePublisher.publish(lastState, sagaId);
    }

    public Set<UUID> getAllUuids() {
        return stopQueues.keySet();
    }
}
