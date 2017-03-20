package sample.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class BasicTransformation2 {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final Materializer materializer = ActorMaterializer.create(system);

    final String text1 =
      "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
      "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, " +
      "when an unknown printer took a galley of type and scrambled it to make a type " +
      "specimen book.";

    String text2 =
            "blah blah blah blah blah blah";

    Integer[] ints = {1,2,3,4,5,6,8,9,10,11,12,13,14,15};

    String text3 =
            "gdf ggrdh s gdf v hhfghrdg greg 5grtgdv g rrgsg";
    System.out.println("start");
    System.out.println("after Flow Gen");
    Source<Integer, NotUsed> numericSource = Source.from(Arrays.asList(ints));
    System.out.println("end");

      Sink<Integer, CompletionStage<Done>> writeAuthors2 = Sink.foreach(s -> System.out.println("sink 3: " + s));
      Sink<Integer, CompletionStage<Done>> writeHashtags2 = Sink.foreach(s -> System.out.println("sink 4: " + s));

      UniqueKillSwitch killSwitch = createInnerGraph(materializer, numericSource, writeAuthors2, writeHashtags2);

      System.out.println("About to shutdown");
      try {
          Thread.sleep(3000);
      } catch (Exception e) {
          
      }
      killSwitch.shutdown();
      System.out.println("shutdown");
//      RunnableGraph.fromGraph(GraphDSL.create(b -> {
//
//          final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = numericSource
//                  .viaMat(KillSwitches.single(), Keep.right())
//                  .toMat(writeHashtags2, Keep.both()).run(materializer);
//          UniqueKillSwitch killSwitch = stream.first();
//
//
//          createInnerGraph(materializer, numericSource, writeAuthors2, writeHashtags2);
//
//          System.out.println("about to kill inner graph");
//          Thread.sleep(2000);
//          killSwitch.shutdown();
//          System.out.println("Killed !!!! ");
//
//          return ClosedShape.getInstance();
//      })).run(materializer);

      try {
          Thread.sleep(5000);
      } catch (Exception e) {

      }
      System.out.println("terminating system");
      system.terminate();

  }

    private static UniqueKillSwitch createInnerGraph(Materializer materializer, Source<Integer, NotUsed> source1, Sink<Integer, CompletionStage<Done>> writeAuthors, Sink<Integer, CompletionStage<Done>> writeHashtags) {
        UniqueKillSwitch killSwitch;

            final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = source1.throttle(1,
                    FiniteDuration.create(1, TimeUnit.SECONDS), 1, ThrottleMode.shaping())
                    .viaMat(KillSwitches.single(), Keep.right())
                    .toMat(writeHashtags, Keep.both()).run(materializer);
          killSwitch = stream.first();


        return killSwitch;
    }


    public static class FlowGenerator{

      Materializer materializer;
      public FlowGenerator(Materializer mat) {
          materializer = mat;
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
              Thread.sleep(5000);
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
