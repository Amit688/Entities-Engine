package org.z.entities.engine;

import java.util.Arrays;
import java.util.HashMap;
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
	private Map<TopicDescriptor, GenericRecord> latestUpdates;
	private TopicDescriptor preferredSon;
	
	public EntityManager(UUID uuid) {
		this.uuid = uuid;
		latestUpdates = new HashMap<>();
		preferredSon = null;
	}

	@Override
	public ProducerRecord<String, GenericRecord> apply(ConsumerRecord<String, Object> record) {
		System.out.println("processing report for uuid " + uuid);
		GenericRecord data = (GenericRecord) record.value();
		TopicDescriptor topicDescriptor = getTopicDescriptor(data);
		preferredSon = topicDescriptor;
		latestUpdates.put(topicDescriptor, data);
		
		GenericRecord guiUpdate = createUpdate();
		return new ProducerRecord<String, GenericRecord>("ui", guiUpdate);
	}
	
	private TopicDescriptor getTopicDescriptor(GenericRecord data) {
		//TODO
		return null;
	}
	
	private GenericRecord createUpdate() {
		//TODO- add all sons, verify field meanings
		GenericRecord son = new GenericRecordBuilder(SINGLE_ENTITY_SCHEMA)
				.set("entityID", uuid.toString())
				.set("entityAttributes", preferredSon)
				.build();
		GenericRecord family = new GenericRecordBuilder(ENTITY_FAMILY_SCHEMA)
				.set("entityID", uuid.toString())
				.set("entityAttributes", preferredSon)
				.set("sons", Arrays.asList(son))
				.build();
		return family;
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
