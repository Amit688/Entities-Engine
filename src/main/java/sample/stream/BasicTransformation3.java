//package sample.stream;
//
//import akka.Done;
//import akka.NotUsed;
//import akka.actor.ActorSystem;
//import akka.japi.Pair;
//import akka.stream.*;
//import akka.stream.javadsl.*;
//import scala.concurrent.duration.FiniteDuration;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.concurrent.CompletionStage;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//
//public class BasicTransformation3 {
//  public static void main(String[] args) throws IOException {
//    final ActorSystem system = ActorSystem.create("Sys");
//    final Materializer materializer = ActorMaterializer.create(system);
//
//    final String text1 =
//      "Lorem Ipsum is simply dummy text of the printing and typesetting industry. " +
//      "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, " +
//      "when an unknown printer took a galley of type and scrambled it to make a type " +
//      "specimen book.";
//
//    String text2 =
//            "blah blah blah blah blah blah";
//
//    Integer[] ints = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
//
//    String text3 =
//            "gdf ggrdh s gdf v hhfghrdg greg 5grtgdv g rrgsg";
//    System.out.println("start");
//    System.out.println("after Flow Gen");
//    Source<Integer, NotUsed> numericSource = Source.from(Arrays.asList(ints));
//    System.out.println("end");
//
//      Sink<Integer, CompletionStage<Done>> writeAuthors2 = Sink.foreach(s -> System.out.println("sink 3: " + s));
//      Sink<Integer, CompletionStage<Done>> writeHashtags2 = Sink.foreach(s -> System.out.println("sink 4: " + s));
//
////      final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = numericSource.throttle(1,
////              FiniteDuration.create(1, TimeUnit.SECONDS), 1, ThrottleMode.shaping())
////              .viaMat(KillSwitches.single(), Keep.right())
////              .toMat(writeHashtags2, Keep.both()).run(materializer);
////      UniqueKillSwitch killSwitch = stream.first();
//
//      UniqueKillSwitch killSwitch = createInnerGraph(materializer, numericSource, writeAuthors2, writeHashtags2);
//
//      System.out.println("About to shutdown");
//      try {
//          Thread.sleep(3000);
//      } catch (Exception e) {
//
//      }
//      killSwitch.shutdown();
//      System.out.println("shutdown");
////      RunnableGraph.fromGraph(GraphDSL.create(b -> {
////
////          final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = numericSource
////                  .viaMat(KillSwitches.single(), Keep.right())
////                  .toMat(writeHashtags2, Keep.both()).run(materializer);
////          UniqueKillSwitch killSwitch = stream.first();
////
////
////          createInnerGraph(materializer, numericSource, writeAuthors2, writeHashtags2);
////
////          System.out.println("about to kill inner graph");
////          Thread.sleep(2000);
////          killSwitch.shutdown();
////          System.out.println("Killed !!!! ");
////
////          return ClosedShape.getInstance();
////      })).run(materializer);
//
//      try {
//          Thread.sleep(5000);
//      } catch (Exception e) {
//
//      }
//      System.out.println("terminating system");
//      system.terminate();
//
//  }
//
//    private static UniqueKillSwitch createInnerGraph(Materializer materializer, Source<Integer, NotUsed> source1, Sink<Integer, CompletionStage<Done>> writeAuthors, Sink<Integer, CompletionStage<Done>> writeHashtags) {
//      final AtomicReference<UniqueKillSwitch> reference = new AtomicReference<>();
//      RunnableGraph.fromGraph(GraphDSL.create(b2 -> {
//            final UniformFanOutShape<Integer, Integer> bcast2 = b2.add(Broadcast.create(2));
//            final FlowShape<Integer, Integer> toAuthor2 =
//                    b2.add(Flow.of(Integer.class).map(t -> {Thread.sleep(1000);return t + 1;}));
//            final FlowShape<Integer, Integer> toTags2 =
//                    b2.add(Flow.of(Integer.class).map(t -> {Thread.sleep(2000);return t + 1;}));
//            final SinkShape<Integer> authors2 = b2.add(writeAuthors);
//            final SinkShape<Integer> hashtags2 = b2.add(writeHashtags);
//
//
//            b2.from(b2.add(source1)).viaMat(KillSwitches.single(), Keep.right()).viaFanOut(bcast2).via(toAuthor2).to(authors2);
//            b2.from(bcast2).via(toTags2).to(hashtags2);
//
//            reference.set(stream.first());
//
//
//
//          final Sink<Integer, CompletionStage<Done>> lastSnk = Sink.foreach(System.out::print);
//          final Pair<UniqueKillSwitch, CompletionStage<Done>> stream = source1
//                  .viaMat(KillSwitches.single(), Keep.right())
//                  .toMat(lastSnk, Keep.both()).run(materializer);
//          reference.set(stream.first());
//          //killSwitch.shutdown();
//
//
//            return ClosedShape.getInstance();
//        })).run(materializer);
//        return reference.get();
//    }
//
//
//    public static class EntitiesSupervisor{
//
//      Materializer materializer;
//      public EntitiesSupervisor(Materializer mat) {
//          materializer = mat;
//      }
//
//      public static void printSleep(String str) {
//          try {
//              Thread.sleep(1000);
//              System.out.println("printSleep: " + str);
//          } catch (Exception e) {
//              System.out.println("exception" + e);
//          }
//      }
//      public static void printSleep2(String str) {
//          try {
//              Thread.sleep(2000);
//              System.out.println("printSleep2: " + str);
//          } catch (Exception e) {
//              System.out.println("exception" + e);
//          }
//      }
//
//      public static void regularPrint(String str) {
//          try {
//              Thread.sleep(5000);
//              long threadId = Thread.currentThread().getId();
//              System.out.println(str + " " + threadId);
//          } catch (Exception e) {
//              System.out.println("exception" + e);
//          }
//      }
//
//        public static Integer IntegerPrint(Integer integer) {
//            try {
//                Thread.sleep(1000);
//                System.out.println("integerPrint: " + integer);
//            } catch (Exception e) {
//                System.out.println("exception" + e);
//            }
//            return integer;
//        }
//
//  }
//
//
//}
