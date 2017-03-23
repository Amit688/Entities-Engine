import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {

	public static void main(String[] args) throws InterruptedException {
		ActorSystem system = ActorSystem.create("avro_producer");
		Materializer materializer = ActorMaterializer.create(system);
		
		CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("https://schema-registry-ui.landoop.com:8081", 1000);
		ProducerSettings<String, Object> producerSettings = ProducerSettings
				.create(system, new StringSerializer(), new KafkaAvroSerializer(client))
				.withBootstrapServers("localhost:9092");
				//.withProperty("schema.registry.url", "https://schema-registry-ui.landoop.com");
		Sink<ProducerRecord<String, Object>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);
		
		String schemaString = "{\"type\": \"record\", "
						+ "\"name\": \"detectionEvent\", "
						+ "\"doc\": \"This is a schema for entity detection report event\", " 
						+ "\"fields\": [{ \"name\": \"sourceName\", "
										+ "\"type\": \"string\", "
										+ "\"doc\" : \"interface name\" }, " 
										+"{ \"name\": \"externalSystemID\", "
										+ "\"type\": \"string\", "
										+ "\"doc\":\"external system ID\"}]}";
		Schema schema = new Schema.Parser().parse(schemaString);
		GenericRecord record = new GenericRecordBuilder(schema)
				.set("sourceName", "source1")
				.set("externalSystemID", "id1")
				.build();
		ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>("detection", record);
		
		Source.from(Arrays.asList(producerRecord))
			.alsoTo(Sink.foreach(r -> System.out.println(r)))
			.to(sink)
			.run(materializer);
		
		Thread.sleep(1000);
		
		system.terminate();
	}
	
	private static class GenericRecordSerializer implements Serializer<GenericRecord> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public byte[] serialize(String topic, GenericRecord data) {
			try {
				DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(data.getSchema());
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				datumWriter.write(data, EncoderFactory.get().directBinaryEncoder(outputStream, null));
				return outputStream.toByteArray();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
		
	}

}
