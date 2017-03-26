package org.z.entities.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
;

public class EntityManager implements Function<ConsumerRecord<String, Object>, ProducerRecord<String, GenericRecord>> {
	private static Schema SINGLE_ENTITY_SCHEMA = getSingleEntitySchema();
	private static Schema ENTITY_FAMILY_SCHEMA = getEntityFamilySchema();
	
	private UUID uuid;
	private Map<SourceDescriptor, GenericRecord> sons;
	private SourceDescriptor preferredSource;
	
	public EntityManager(UUID uuid) {
		this.uuid = uuid;
		sons = new HashMap<>();
		preferredSource = null;
	}

	@Override
	public ProducerRecord<String, GenericRecord> apply(ConsumerRecord<String, Object> record) {
		System.out.println("processing report for uuid " + uuid);
		GenericRecord data = (GenericRecord) record.value();
		SourceDescriptor sourceDescriptor = getSourceDescriptor(record);
		preferredSource = sourceDescriptor;
		sons.put(sourceDescriptor, data);
		
		GenericRecord guiUpdate = createUpdate();
		return new ProducerRecord<String, GenericRecord>("ui", guiUpdate);
	}
	
	private SourceDescriptor getSourceDescriptor(ConsumerRecord<String, Object> record) {
		//TODO
		return null;
	}
	
	private GenericRecord createUpdate() {
		//TODO- verify meaning of entityID
		List<GenericRecord> sonsRecords = new ArrayList<>();
		for (GenericRecord son : sons.values()) {
			sonsRecords.add(createSingleEntityUpdate(son));
		}
		GenericRecord family = new GenericRecordBuilder(ENTITY_FAMILY_SCHEMA)
				.set("entityID", uuid.toString())
				.set("entityAttributes", sons.get(preferredSource))
				.set("sons", sonsRecords)
				.build();
		return family;
	}
	
	private GenericRecord createSingleEntityUpdate(GenericRecord latestUpdate) {
		return new GenericRecordBuilder(SINGLE_ENTITY_SCHEMA)
				.set("entityID", uuid.toString())
				.set("entityAttributes", latestUpdate)
				.build();
	}
	
	private static Schema getSingleEntitySchema() {
		return new Schema.Parser().parse(
				"{\"type\": \"record\", \"name\": \"systemEntity\",\"doc\": \"This is a schema of a single processed entity with all attributes.\","
				+ "\"fields\": [{\"name\": \"entityID\", \"type\": \"string\"}, "
							+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"}]}");
	}
	
	private static Schema getEntityFamilySchema() {
		return new Schema.Parser().parse(
				"{\"type\": \"record\", \"name\": \"entityFamily\", \"doc\": \"This is a schema of processed entity with full attributes.\","
				+ "\"fields\": [{\"name\": \"entityID\", \"type\": \"string\"},"
							+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"},"
							+ "{\"name\" : \"sons\", \"type\":"
															+ "[{\"type\": \"array\", \"items\": {\"name\": \"entity\", \"type\": \"systemEntity\"}}]"
							+ "}]}");
	}

}
