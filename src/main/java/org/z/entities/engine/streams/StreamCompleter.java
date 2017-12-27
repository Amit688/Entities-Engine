package org.z.entities.engine.streams;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.z.entities.engine.EntitiesSupervisor;
import org.z.entities.engine.utils.Utils;

import java.util.UUID;

public class StreamCompleter extends GraphStage<FlowShape<ConsumerRecord<Object, Object>, ConsumerRecord<Object, Object>>> {
    private EntitiesSupervisor entitiesSupervisor;
    private EntityProcessor entityProcessor;
    private UUID uuid;
    
	final static public Logger logger = Logger.getLogger(StreamCompleter.class);
	static {
		Utils.setDebugLevel(logger);
	}

    

    public StreamCompleter(EntitiesSupervisor entitiesSupervisor, EntityProcessor entityProcessor) {
        this.entitiesSupervisor = entitiesSupervisor;
        this.entityProcessor = entityProcessor;
        this.uuid = entityProcessor.getUuid();
    }

    public final Inlet<ConsumerRecord<Object, Object>> in = Inlet.create("Filter.in");
    public final Outlet<ConsumerRecord<Object, Object>> out = Outlet.create("Filter.out");

    private final FlowShape<ConsumerRecord<Object, Object>, ConsumerRecord<Object, Object>> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<ConsumerRecord<Object, Object>, ConsumerRecord<Object, Object>> shape() {
        return shape;
    }

    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() {
                    	ConsumerRecord<Object, Object> input = grab(in); 
                         logger.debug("Completer " + uuid + " is doing push " + input.value());
                    	GenericRecord record = (GenericRecord) input.value();
                        if (record.getSchema().getName().equals("stopMeMessage")) {
                        	logger.debug("Completer " + uuid + " detected stop message");
                            notifyEntitiesSupervisor(record);
                            completeStage();
                        } else {
                            push(out, input);
                        }
                    }

                    private void notifyEntitiesSupervisor(GenericRecord record) {
                        UUID sagaId = null;
                        Object sagaIdAsObject = record.get("sagaId");
                        if (sagaIdAsObject != null) {
                            sagaId = UUID.fromString(sagaIdAsObject.toString());
                        }

                        UUID uuid = entityProcessor.getUuid();
                        GenericRecord currentState = entityProcessor.getCurrentState();
                        entitiesSupervisor.notifyOfStreamCompletion(uuid, currentState, sagaId);
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                    	logger.debug("Completer " + uuid + " is being pulled");
                        pull(in);
                    }
                });
            }
        };
    }
}
