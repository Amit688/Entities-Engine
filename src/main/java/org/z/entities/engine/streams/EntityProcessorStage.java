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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger; 
import org.z.entities.engine.utils.Utils;

public class EntityProcessorStage extends GraphStage<FlowShape<GenericRecord, ProducerRecord<Object, Object>>> {

    public final Inlet<GenericRecord> in = Inlet.create("Processor.in");
    public final Outlet<ProducerRecord<Object, Object>> out = Outlet.create("Processor.out");
    private EntityProcessor entityProcessor;
    private boolean sendInitialState;
	final static public Logger logger = Logger.getLogger(EntityProcessorStage.class);
	static {
		Utils.setDebugLevel(logger);
	}   

    public EntityProcessorStage(EntityProcessor entityProcessor, boolean sendInitialState) {
        this.entityProcessor = entityProcessor;
        this.sendInitialState = sendInitialState;
        logger.debug(sendInitialState);
    }

    private final FlowShape<GenericRecord, ProducerRecord<Object, Object>> shape = FlowShape.of(in, out);
    @Override
    public FlowShape<GenericRecord, ProducerRecord<Object, Object>> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {
            private boolean alreadySentInitialState = false;

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        push(out, entityProcessor.apply(grab(in)));
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if (sendInitialState && !alreadySentInitialState) {
                        	ProducerRecord<Object, Object> producerRecord = entityProcessor.generateGuiUpdate();
                        	logger.debug("sending initial state - " + producerRecord);
                            push(out, producerRecord);
                            alreadySentInitialState = true;
                        } else {
                        	logger.debug("pulling new state");
                            pull(in);
                        }
                    }
                });
            }
        };
    }
}
