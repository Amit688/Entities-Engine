package sample.stream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import akka.NotUsed;
import akka.actor.SystemGuardian;
import akka.stream.Materializer;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import scala.Int;
import scala.Symbol;
import scala.runtime.BoxedUnit;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.SinkShape;
import akka.stream.ClosedShape;
import akka.stream.Graph;
import akka.Done;
import akka.stream.UniqueKillSwitch;
import akka.stream.KillSwitches;
import akka.japi.Pair;

public class BasicTransformation {
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
    FlowGenerator generator = new FlowGenerator(materializer);
    System.out.println("after Flow Gen");
    Source<String, NotUsed> source1 = Source.from(Arrays.asList(text1.split("\\s")));
    Source<Integer, NotUsed> numericSource = Source.from(Arrays.asList(ints));
//    Source.from(Arrays.asList(text1.split("\\s"))).
////       transform
//      map(e -> e.toUpperCase()).via(generator.get()).runWith(Sink.foreach(EntitiesSupervisor::regularPrint), materializer);
    System.out.println("end");

      Sink<String, CompletionStage<Done>> writeAuthors = Sink.foreach(FlowGenerator::printSleep);
      Sink<String, CompletionStage<Done>> writeHashtags = Sink.foreach(FlowGenerator::printSleep2);
      Sink<Integer, CompletionStage<Done>> writeAuthors2 = Sink.foreach(s -> System.out.println("sink 3: " + s));
      Sink<Integer, CompletionStage<Done>> writeHashtags2 = Sink.foreach(s -> System.out.println("sink 4: " + s));
      RunnableGraph.fromGraph(GraphDSL.create(b -> {
//          final UniformFanOutShape<String, String> bcast = b.add(Broadcast.create(2));
//          final FlowShape<String, String> toAuthor =
//                  b.add(Flow.of(String.class).map(t -> t + "1"));
//          final FlowShape<String, String> toTags =
//                  b.add(Flow.of(String.class).map(t -> t + "2"));
//          final SinkShape<String> authors = b.add(writeAuthors);
//          final SinkShape<String> hashtags = b.add(writeHashtags);


//          b.from(b.add(source1)).viaFanOut(bcast).via(toAuthor).to(authors);
//          b.from(bcast).via(toTags).to(hashtags);


          final Sink<Integer, CompletionStage<Done>> lastSnk = Sink.foreach(FlowGenerator::IntegerPrint);
          final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = numericSource
                  .viaMat(KillSwitches.single(), Keep.right())
                  .toMat(lastSnk, Keep.both()).run(materializer);
          UniqueKillSwitch killSwitch = stream.first();


          createInnerGraph(materializer, numericSource, writeAuthors2, writeHashtags2);

          System.out.println("about to kill inner graph");
          Thread.sleep(2000);
          killSwitch.shutdown();
          System.out.println("Killed !!!! ");

          return ClosedShape.getInstance();
      })).run(materializer);


  }

    private static void createInnerGraph(Materializer materializer, Source<Integer, NotUsed> source1, Sink<Integer, CompletionStage<Done>> writeAuthors, Sink<Integer, CompletionStage<Done>> writeHashtags) {
        RunnableGraph.fromGraph(GraphDSL.create(b2 -> {
            final UniformFanOutShape<Integer, Integer> bcast2 = b2.add(Broadcast.create(2));
            final FlowShape<Integer, Integer> toAuthor2 =
                    b2.add(Flow.of(Integer.class).map(t -> {Thread.sleep(1000);return t + 1;}));
            final FlowShape<Integer, Integer> toTags2 =
                    b2.add(Flow.of(Integer.class).map(t -> {Thread.sleep(2000);return t + 1;}));
            final SinkShape<Integer> authors2 = b2.add(writeAuthors);
            final SinkShape<Integer> hashtags2 = b2.add(writeHashtags);


            b2.from(b2.add(source1)).viaFanOut(bcast2).via(toAuthor2).to(authors2);
            b2.from(bcast2).via(toTags2).to(hashtags2);


//          final Sink<Integer, CompletionStage<Done>> lastSnk = Sink.foreach(System.out::print);
//          final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = source1
//                  .viaMat(KillSwitches.single(), Keep.right())
//                  .toMat(lastSnk, Keep.both()).run(materializer);
//          UniqueKillSwitch killSwitch = stream.first();
//          killSwitch.shutdown();


            return ClosedShape.getInstance();
        })).run(materializer);
    }


    public static class FlowGenerator implements Supplier<Flow<String, String, NotUsed>>{

      Materializer materializer;
      public FlowGenerator(Materializer mat) {
          materializer = mat;
      }
      @Override
      public Flow<String, String, NotUsed> get() {
          System.out.println("creating flow");
          return Flow.<String>create().alsoTo(Sink.foreach(System.out::println)).map(s -> s + "- supervisor");
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
