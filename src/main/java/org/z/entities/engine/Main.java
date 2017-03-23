package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;

import akka.actor.ActorSystem;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
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
		
		Source<EntitiesEvent, Consumer.Control> detectionEvents = 
				KafkaSourceFactory.create(system, "detection")
				.via(Flow.fromFunction(r -> (GenericRecord) r.value()))
				.via(Flow.fromFunction(r -> new EntitiesEvent(EntitiesEvent.Type.CREATE, r)));
    	detectionEvents
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
}
