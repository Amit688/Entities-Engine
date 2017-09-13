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
import org.z.entities.engine.EntitiesSupervisor;

import java.util.UUID;

public class StreamCompleter extends GraphStage<FlowShape<GenericRecord, GenericRecord>> {
    private EntitiesSupervisor entitiesSupervisor;
    private EntityProcessor entityProcessor;
    private UUID uuid;

    public StreamCompleter(EntitiesSupervisor entitiesSupervisor, EntityProcessor entityProcessor) {
        this.entitiesSupervisor = entitiesSupervisor;
        this.entityProcessor = entityProcessor;
        this.uuid = entityProcessor.getUuid();
    }

    public final Inlet<GenericRecord> in = Inlet.create("Filter.in");
    public final Outlet<GenericRecord> out = Outlet.create("Filter.out");

    private final FlowShape<GenericRecord, GenericRecord> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<GenericRecord, GenericRecord> shape() {
        return shape;
    }

    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() {
                        GenericRecord record = grab(in);
//                        System.out.println("Completer " + uuid + " is doing push " + record);
                        if (record.getSchema().getName().equals("stopMeMessage")) {
//                            System.out.println("Completer " + uuid + " detected stop message");
                            notifyEntitiesSupervisor(record);
                            completeStage();
                        } else {
                            push(out, record);
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
//                        System.out.println("Completer " + uuid + " is being pulled");
                        pull(in);
                    }
                });
            }
        };
    }
}
