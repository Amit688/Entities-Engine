package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
    	final ActorSystem system = ActorSystem.create();
		final ActorMaterializer materializer = ActorMaterializer.create(system);
		EntitiesSupervisor supervisor = new EntitiesSupervisor(system, materializer);
		
		Source<EntitiesEvent, ?> detectionsSource = createSourceWithType(system, "detection", EntitiesEvent.Type.CREATE);
		Source<EntitiesEvent, ?> mergesSource = createSourceWithType(system, "merge", EntitiesEvent.Type.MERGE);
		Source<EntitiesEvent, ?> splitsSource = createSourceWithType(system, "split", EntitiesEvent.Type.SPLIT);
		Source<EntitiesEvent, ?> combinedSource = Source.fromGraph(GraphDSL.create(builder -> {
			UniformFanInShape<EntitiesEvent, EntitiesEvent> merger = builder.add(Merge.create(3));
			directToMerger(builder, detectionsSource, merger);
			directToMerger(builder, mergesSource, merger);
			directToMerger(builder, splitsSource, merger);
			return SourceShape.of(merger.out());
		}));
		
		combinedSource
    		.alsoTo(Sink.foreach(r -> System.out.println(r)))
    		.to(Sink.foreach(supervisor::accept))
    		.run(materializer);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				system.terminate();
			}
		});
		System.out.println("Ready");
		while(true) {
			Thread.sleep(3000);
		}
    }
    
    private static Source<EntitiesEvent, ?> createSourceWithType(ActorSystem system, String topic, EntitiesEvent.Type type) {
    	return KafkaSourceFactory.create(system, topic)
    			.via(Flow.fromFunction(r -> new EntitiesEvent(type, (GenericRecord) r.value())));
    }
    
    private static void directToMerger(Builder<NotUsed> builder, 
    		Source<EntitiesEvent, ?> source, UniformFanInShape<EntitiesEvent, ?> merger) {
    	builder.from(builder.add(source).out()).toFanIn(merger);
    }
}
