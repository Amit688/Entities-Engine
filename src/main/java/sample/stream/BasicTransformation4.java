package sample.stream;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Consumer;


import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import scala.concurrent.duration.FiniteDuration;

public class BasicTransformation4 {
    public static void main(String[] args) throws IOException {
        final ActorSystem system = ActorSystem.create("Sys");
        final Materializer materializer = ActorMaterializer.create(system);

        final String text1 =
                "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
                        "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, " +
                        "when an unknown printer took a galley of type and scrambled it to make a type " +
                        "specimen book.";

        Integer[] ints = {1,2,3,4,5,6,8,9,10,11,12,13,14,15};
        final String text2 =
                "A B C";

//        Source<String, NotUsed> source1 = Source.from(Arrays.asList(text1.split("\\s")));
        Source<Integer, NotUsed> source1 = Source.from(Arrays.asList(ints));
        EntitiesSupervisor generator = new EntitiesSupervisor(materializer, source1);
        Source.from(Arrays.asList(text2.split("\\s")))
                .runWith(Sink.foreach(t -> generator.accept(t)), materializer);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }
        System.out.println("KILLING TIME !!!!");
        generator.kill("B");
    }

    public static class EntitiesSupervisor implements Consumer<String> {

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

        public static void printSleep(String str) {
            try {
                Thread.sleep(1000);
                System.out.println("printSleep: " + str);
            } catch (Exception e) {
                System.out.println("exception" + e);
            }
        }
        public static void printSleep2(String str) {
            try {
                Thread.sleep(2000);
                System.out.println("printSleep2: " + str);
            } catch (Exception e) {
                System.out.println("exception" + e);
            }
        }

        public static void regularPrint(String str) {
            try {
//                Thread.sleep(5000);
                long threadId = Thread.currentThread().getId();
                System.out.println(str + " " + threadId);
            } catch (Exception e) {
                System.out.println("exception" + e);
            }
        }

        public static Integer IntegerPrint(Integer integer) {
            try {
                Thread.sleep(1000);
                System.out.println("integerPrint: " + integer);
            } catch (Exception e) {
                System.out.println("exception" + e);
            }
            return integer;
        }

    }


}
