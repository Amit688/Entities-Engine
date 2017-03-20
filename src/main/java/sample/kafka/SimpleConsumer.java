package sample.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class SimpleConsumer {

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-consumer-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        Serde<String> stringSerde = Serdes.String();
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "test");
		textLines.foreach((key, value) -> process(value));
		
		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
		
		Thread.sleep(5000);
		System.out.println("stopping");
		streams.close();
	}
	
	private static void process(String value) {
		System.out.println(value);
	}

}
