package org.z.entities.engine.streams;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.AsyncCallback;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.apache.avro.generic.GenericRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class InterfaceSource extends GraphStage<SourceShape<GenericRecord>> {
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public final Outlet<GenericRecord> out = Outlet.create("InterfaceSource.out");
    private final SourceShape<GenericRecord> shape = SourceShape.of(out);

    private BlockingQueue<GenericRecord> queue;
    private String identifier;

    public InterfaceSource(BlockingQueue<GenericRecord> queue, String identifier) {
        this.queue = queue;
        this.identifier = identifier;
    }

    @Override
    public SourceShape<GenericRecord> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {
            private AsyncCallback<GenericRecord> callBack;
            private Future<?> taskFuture;

            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        GenericRecord record = queue.poll();
                        if (record == null) {
//                            System.out.println("InterfaceSource " + identifier + " no records in queue, invoking callback");
                            taskFuture = executor.submit(() -> invokeCallbackWhenReady()); // necessary, otherwise blocks upstream
                        } else {
//                            System.out.println("InterfaceSource " + identifier + " found record during pull, pushing");
                            push(out, record);
                        }
                    }

                    @Override
                    public void onDownstreamFinish() {
                        if (taskFuture != null) {
                            taskFuture.cancel(true);
                        }
                    }
                });
            }

            @Override
            public void preStart() {
                callBack = createAsyncCallback(record -> {
//                    System.out.println("InterfaceSource " + identifier + " trying to push during callback");
                    push(out, record);
//                    System.out.println("InterfaceSource " + identifier + " pushed during callback");
                });
            }

            private void invokeCallbackWhenReady() {
                GenericRecord record = null;
                while (record == null) {
                    try {
                        record = queue.poll(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        return; // Probably interrupted by onDownstreamFinish().
                    }
                }
//                System.out.println("InterfaceSource " + identifier + " found record during task, invoking callback");
                callBack.invoke(record);
            }
        };
    }
}

