package org.z.entities.engine;

import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EntityManager implements Function<ConsumerRecord<String, GenericRecord>, GenericRecord> {
	private UUID uuid;
	
	public EntityManager(UUID uuid) {
		this.uuid = uuid;
	}

	@Override
	public GenericRecord apply(ConsumerRecord<String, GenericRecord> record) {
		System.out.println("processing report for uuid " + uuid);
		return record.value();
	}

}
