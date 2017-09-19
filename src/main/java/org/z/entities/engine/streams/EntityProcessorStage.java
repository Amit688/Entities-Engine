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

public class EntityProcessorStage extends GraphStage<FlowShape<GenericRecord, ProducerRecord<Object, Object>>> {

    public final Inlet<GenericRecord> in = Inlet.create("Processor.in");
    public final Outlet<ProducerRecord<Object, Object>> out = Outlet.create("Processor.out");

    private EntityProcessor entityProcessor;
    private boolean sendInitialState;

    public EntityProcessorStage(EntityProcessor entityProcessor, boolean sendInitialState) {
        this.entityProcessor = entityProcessor;
        this.sendInitialState = sendInitialState;
        System.out.println(sendInitialState);
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
//                            System.out.println("sending initial state");
                            push(out, entityProcessor.generateGuiUpdate());
                            alreadySentInitialState = true;
                        } else {
//                            System.out.println("pulling new state");
                            pull(in);
                        }
                    }
                });
            }
        };
    }
}
