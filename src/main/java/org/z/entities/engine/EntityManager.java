package org.z.entities.engine;

import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EntityManager implements Function<ConsumerRecord<String, Object>, ProducerRecord<String, GenericRecord>> {
	private UUID uuid;
	
	public EntityManager(UUID uuid) {
		this.uuid = uuid;
	}

	@Override
	public ProducerRecord<String, GenericRecord> apply(ConsumerRecord<String, Object> record) {
		System.out.println("processing report for uuid " + uuid);
		return new ProducerRecord<String, GenericRecord>("ui", (GenericRecord) record.value());
	}

}
