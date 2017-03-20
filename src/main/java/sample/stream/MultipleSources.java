package sample.stream;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.Graph;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.ThrottleMode;
import akka.stream.UniformFanInShape;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import scala.concurrent.duration.FiniteDuration;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

/**
 * @author akir94
 *
 */
public class MultipleSources {

	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create("multipleSources");
		ActorMaterializer materializer = ActorMaterializer.create(system);
		
		Graph<ClosedShape, ?> graph = GraphDSL.create(MultipleSources::initGraphBuilder);
		RunnableGraph<?> runnable = RunnableGraph.fromGraph(graph);
		runnable.run(materializer);
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		system.terminate();
	}
	
	private static ClosedShape initGraphBuilder(Builder<NotUsed> builder) {
		Source<Integer, NotUsed> source1 = Source.range(100, 199).throttle(1, FiniteDuration.create(5, TimeUnit.MILLISECONDS), 1, ThrottleMode.shaping());
		Source<Integer, NotUsed> source2 = Source.range(200, 299).throttle(1, FiniteDuration.create(10, TimeUnit.MILLISECONDS), 1, ThrottleMode.shaping());
		Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(i -> System.out.println(i));
		
		UniformFanInShape<Integer, Integer> merger = builder.add(Merge.create(2));
		Outlet<Integer> outlet1 = builder.add(source1).out();
		Outlet<Integer> outlet2 = builder.add(source2).out();
		Inlet<Integer> inlet = builder.add(sink).in();
		
		builder.from(outlet1).viaFanIn(merger).toInlet(inlet);
		builder.from(outlet2).toFanIn(merger);
		
		return ClosedShape.getInstance();
	}

}
