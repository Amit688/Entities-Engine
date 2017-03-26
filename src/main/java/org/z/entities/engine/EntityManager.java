package org.z.entities.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
;

public class EntityManager implements Function<ConsumerRecord<String, Object>, ProducerRecord<String, GenericRecord>> {
	private UUID uuid;
	private SchemaRegistryClient schemaRegistry;
	private Map<SourceDescriptor, GenericRecord> sons;
	private SourceDescriptor preferredSource;
	
	public EntityManager(UUID uuid, SchemaRegistryClient schemaRegistry) {
		this.uuid = uuid;
		this.schemaRegistry = schemaRegistry;
		sons = new HashMap<>();
		preferredSource = null;
	}

	@Override
	public ProducerRecord<String, GenericRecord> apply(ConsumerRecord<String, Object> record) {
		try {
			System.out.println("processing report for uuid " + uuid);
			GenericRecord data = (GenericRecord) record.value();
			SourceDescriptor sourceDescriptor = getSourceDescriptor(record.topic(), data);
			preferredSource = sourceDescriptor;
			sons.put(sourceDescriptor, data);
			try {
				GenericRecord guiUpdate = createUpdate();
				return new ProducerRecord<String, GenericRecord>("ui", guiUpdate);
			} catch (IOException | RestClientException e) {
				System.out.println("failed to generate update to ui");
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	private SourceDescriptor getSourceDescriptor(String topic, GenericRecord data) {
		return new SourceDescriptor(topic, data.get("externalSystemID").toString());
	}
	
	private GenericRecord createUpdate() throws IOException, RestClientException {
		//TODO- verify meaning of entityID
		List<GenericRecord> sonsRecords = new ArrayList<>();
		for (GenericRecord son : sons.values()) {
			sonsRecords.add(createSingleEntityUpdate(son));
		}
		int id = schemaRegistry.getLatestSchemaMetadata("entityFamily").getId();
		GenericRecord family = new GenericRecordBuilder(schemaRegistry.getByID(id))
				.set("entityID", uuid.toString())
				.set("entityAttributes", sons.get(preferredSource))
				.set("sons", sonsRecords)
				.build();
		return family;
	}
	
	private GenericRecord createSingleEntityUpdate(GenericRecord latestUpdate) throws IOException, RestClientException {
		int id = schemaRegistry.getLatestSchemaMetadata("systemEntity").getId();
		return new GenericRecordBuilder(schemaRegistry.getByID(id))
				.set("entityID", uuid.toString())
				.set("entityAttributes", latestUpdate)
				.build();
	}

}
