package sample.stream;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Graph;
import akka.stream.KillSwitches;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.ThrottleMode;
import akka.stream.UniformFanInShape;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.concurrent.duration.FiniteDuration;

public class GraphKillSwitch {
	public static void main(String[] args) throws InterruptedException {
		ActorSystem system = ActorSystem.create("graphKillSwitch");
		ActorMaterializer materializer = ActorMaterializer.create(system);
		
		Graph<SourceShape<Integer>, NotUsed> sourceGraph = GraphDSL.create(GraphKillSwitch::initGraphBuilder);
		Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(i -> System.out.println(i));
		
		Pair<UniqueKillSwitch, CompletionStage<Done>> result = Source.fromGraph(sourceGraph)
			.viaMat(KillSwitches.single(), Keep.right())
			.toMat(sink, Keep.both())
			.run(materializer);
		
		
		Thread.sleep(3000);
		result.first().shutdown();

		Thread.sleep(1000);
		system.terminate();
	}
	
	private static SourceShape<Integer> initGraphBuilder(Builder<NotUsed> builder) {
		Source<Integer, NotUsed> source1 = Source.range(100, 199).throttle(1, FiniteDuration.create(500, TimeUnit.MILLISECONDS), 1, ThrottleMode.shaping());
		Source<Integer, NotUsed> source2 = Source.range(200, 299).throttle(1, FiniteDuration.create(250, TimeUnit.MILLISECONDS), 1, ThrottleMode.shaping());
		
		UniformFanInShape<Integer, Integer> merger = builder.add(Merge.create(2));
		Outlet<Integer> outlet1 = builder.add(source1).out();
		Outlet<Integer> outlet2 = builder.add(source2).out();
		
		builder.from(outlet1).viaFanIn(merger);
		builder.from(outlet2).toFanIn(merger);
		
		return SourceShape.of(merger.out());
		
	}

}
