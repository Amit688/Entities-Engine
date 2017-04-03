package org.z.entities.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class EntityManager implements Function<ConsumerRecord<String, Object>, ProducerRecord<String, Object>> {
	private static Schema SYSTEM_ENTITY_SCHEMA = null;
	private static Schema ENTITY_FAMILY_SCHEMA = null;
	private static Schema STATE_CHANGES_SCHEMA = null;
	
	private UUID uuid;
	private SchemaRegistryClient schemaRegistry;
	private Map<SourceDescriptor, GenericRecord> sons;
	private SourceDescriptor preferredSource;
	private GenericData.EnumSymbol stateChange;
	private boolean isInitialStateSent = false;
	
	public EntityManager(UUID uuid, SchemaRegistryClient schemaRegistry, String StateChange) {
		this.uuid = uuid;
		this.schemaRegistry = schemaRegistry;
		sons = new HashMap<>();
		preferredSource = null;
		stateChange = new GenericData.EnumSymbol(STATE_CHANGES_SCHEMA, StateChange);
		registerSchemas();
	}

	@Override
	public ProducerRecord<String, Object> apply(ConsumerRecord<String, Object> record) {
		try {
			System.out.println("processing report for uuid " + uuid + "\nI have " + sons.size() + " sons");
			System.out.println("sons are:");
			for (SourceDescriptor e: sons.keySet())
				System.out.println(e.getSystemUUID());
			GenericRecord data = (GenericRecord) record.value();
			String externalSystemID = data.get("externalSystemID").toString();
			String systemUUID = "";
			for (SourceDescriptor e: sons.keySet())
				if (e.getReportsId().equals(externalSystemID))
					systemUUID = e.getSystemUUID();
			SourceDescriptor sourceDescriptor = getSourceDescriptor(record.topic(), data, systemUUID);
			preferredSource = sourceDescriptor;
			sons.put(sourceDescriptor, data);
			try {
				GenericRecord guiUpdate = createUpdate();
				return new ProducerRecord<String, Object>("update", guiUpdate);
			} catch (IOException | RestClientException e) {
				System.out.println("failed to generate update");
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	private SourceDescriptor getSourceDescriptor(String topic, GenericRecord data, String systemUUID) {
		return new SourceDescriptor(topic, data.get("externalSystemID").toString(), systemUUID);
	}
	
	private GenericRecord createUpdate() throws IOException, RestClientException {
		//TODO- verify meaning of entityID
		List<GenericRecord> sonsRecords = new ArrayList<>();
		for (SourceDescriptor sonKey : sons.keySet()) {
			sonsRecords.add(createSingleEntityUpdate(sons.get(sonKey), sonKey.getSystemUUID()));
		}
		GenericRecord family = new GenericRecordBuilder(ENTITY_FAMILY_SCHEMA)
				.set("entityID", uuid.toString())
				.set("entityAttributes", sons.get(preferredSource))
				.set("sons", sonsRecords)
				.set("stateChanges", stateChange)
				.build();
		if (isInitialStateSent) {
			isInitialStateSent = true;
			stateChange = new GenericData.EnumSymbol(STATE_CHANGES_SCHEMA, "NONE");
		}
		return family;
	}
	
	private GenericRecord createSingleEntityUpdate(GenericRecord latestUpdate, String systemUUID) throws IOException, RestClientException {
		return new GenericRecordBuilder(SYSTEM_ENTITY_SCHEMA)
				.set("entityID", systemUUID)
				.set("entityAttributes", latestUpdate)
				.build();
	}
	
	private void registerSchemas() {
		Schema.Parser parser = new Schema.Parser();
		parser.parse("{\"type\": \"record\","
				+ "\"name\": \"basicEntityAttributes\","
				+ "\"doc\": \"This is a schema for basic entity attributes, this will represent basic entity in all life cycle\","
				+ "\"fields\": ["
					+ "{\"name\": \"coordinate\", \"type\":"
							+ "{\"type\": \"record\","
							+ "\"name\": \"coordinate\","
							+ "\"doc\": \"Location attribute in grid format\","
							+ "\"fields\": ["
								+ "{\"name\": \"lat\",\"type\": \"double\"},"
								+ "{\"name\": \"long\",\"type\": \"double\"}"
							+ "]}},"
					+ "{\"name\": \"isNotTracked\",\"type\": \"boolean\"},"
					+ "{\"name\": \"entityOffset\",\"type\": \"long\"}"
				+ "]}");
		parser.parse("{\"type\": \"record\", "
				+ "\"name\": \"generalEntityAttributes\","
				+ "\"doc\": \"This is a schema for general entity before acquiring by the system\","
				+ "\"fields\": ["
					+ "{\"name\": \"basicAttributes\",\"type\": \"basicEntityAttributes\"},"
					+ "{\"name\": \"speed\",\"type\": \"double\",\"doc\" : \"This is the magnitude of the entity's velcity vector.\"},"
					+ "{\"name\": \"elevation\",\"type\": \"double\"},"
					+ "{\"name\": \"course\",\"type\": \"double\"},"
					+ "{\"name\": \"nationality\",\"type\": {\"name\": \"nationality\", \"type\": \"enum\",\"symbols\" : [\"ISRAEL\", \"USA\", \"SPAIN\"]}},"
					+ "{\"name\": \"category\",\"type\": {\"name\": \"category\", \"type\": \"enum\",\"symbols\" : [\"airplane\", \"boat\"]}},"
					+ "{\"name\": \"pictureURL\",\"type\": \"string\"},"
					+ "{\"name\": \"height\",\"type\": \"double\"},"
					+ "{\"name\": \"nickname\",\"type\": \"string\"},"
					+ "{\"name\": \"externalSystemID\",\"type\": \"string\",\"doc\" : \"This is ID given be external system.\"}"
				+ "]}");
		if (SYSTEM_ENTITY_SCHEMA == null) {
			SYSTEM_ENTITY_SCHEMA = parser.parse("{\"type\": \"record\", "
					+ "\"name\": \"systemEntity\","
					+ "\"doc\": \"This is a schema of a single processed entity with all attributes.\","
					+ "\"fields\": ["
						+ "{\"name\": \"entityID\", \"type\": \"string\"}, "
						+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"}"
					+ "]}");
		}
		if (STATE_CHANGES_SCHEMA == null) {
			STATE_CHANGES_SCHEMA = parser.parse("{\"type\": \"enum\", "
					+ "\"name\": \"stateChanges\", "
					+ "symbols\":[\"MERGED\", \"WAS_SPLIT\", \"SON_TAKEN\", \"NONE\"]}");
		}
		if (ENTITY_FAMILY_SCHEMA == null) {
			ENTITY_FAMILY_SCHEMA = parser.parse("{\"type\": \"record\", "
    				+ "\"name\": \"entityFamily\", "
    				+ "\"doc\": \"This is a schema of processed entity with full attributes.\","
    				+ "\"fields\": ["
    					+ "{\"name\": \"entityID\", \"type\": \"string\"},"
						+ "{\"name\": \"stateChanges\",\"type\": \"stateChanges\"},"
						+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"},"
						+ "{\"name\" : \"sons\", \"type\": [{\"type\": \"array\", \"items\": \"systemEntity\"}]}"
					+ "]}");
		}
	}

}
