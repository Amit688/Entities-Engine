package org.z.entities.engine;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Created by Amit on 20/03/2017.
 */
public class EntitiesSupervisor implements Consumer<String> {

    Materializer materializer;
    Source<Integer, NotUsed> source;
    Map<String, UniqueKillSwitch> killSwitches;
    public EntitiesSupervisor(Materializer mat, Source src) {
        materializer = mat;
        source = src;
        killSwitches = new HashMap<>();
    }

    public void kill(String key) {
        System.out.println(killSwitches.size());
        killSwitches.get(key).shutdown();
    }

    @Override
    public void accept(String message) {
        System.out.println("creating flow");
        Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(t -> {System.out.println("pipe " + message + ": " + t); Thread.sleep(1000);});
        final Pair<UniqueKillSwitch, CompletionStage<Done>> result = source
                .viaMat(KillSwitches.single(), Keep.right())
                .toMat(sink, Keep.both())
                .run(materializer);
        System.out.println("adding to HashMap, " + message);
        killSwitches.put(message, result.first());
        System.out.println("after adding to HashMap" + killSwitches.size());
    }

}