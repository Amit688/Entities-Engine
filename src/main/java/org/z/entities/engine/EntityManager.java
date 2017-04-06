package org.z.entities.engine;

import java.io.IOException;
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

public class EntityManager implements Function<ConsumerRecord<String, Object>, ProducerRecord<String, Object>> {
	private static Schema SYSTEM_ENTITY_SCHEMA = null;
	private static Schema ENTITY_FAMILY_SCHEMA = null;
	
	private UUID uuid;
	private Map<SourceDescriptor, GenericRecord> sons;
	private SourceDescriptor preferredSource;
	private String stateChange;

	public EntityManager(UUID uuid, String StateChange, List<SourceDescriptor> sources) {
		this.uuid = uuid;
		this.stateChange = StateChange;
		initSons(sources);
		preferredSource = null;
		registerSchemas();
	}

	private void initSons(List<SourceDescriptor> sources) {
		sons = new HashMap<>();
		for (SourceDescriptor source : sources) {
			sons.put(source, null);
		}
	}

	public EntityManager(UUID uuid, String StateChange, Map<SourceDescriptor, GenericRecord> sonsAttributes) {
		this.uuid = uuid;
		this.stateChange = StateChange;
		initSons(sonsAttributes);
		preferredSource = null;
		registerSchemas();
	}

	private void initSons(Map<SourceDescriptor, GenericRecord> sonsAttributes) {
		sons = new HashMap<>();
		for (Map.Entry<SourceDescriptor, GenericRecord> sonAttributes : sonsAttributes.entrySet()) {
			sons.put(sonAttributes.getKey(), sonAttributes.getValue());
		}
	}

	@Override
	public ProducerRecord<String, Object> apply(ConsumerRecord<String, Object> record) {
		try {
			System.out.println("processing report for uuid " + uuid + "\nI have " + sons.size() + " sons");
			System.out.println("sons are:");
			for (SourceDescriptor e: sons.keySet())
				System.out.println("system: " + e.getSystemUUID() + ", Reports ID: " + e.getReportsId() + ",  SensorID" + e.getSensorId());
			GenericRecord data = (GenericRecord) record.value();
			SourceDescriptor sourceDescriptor = getSourceDescriptor(data);
			preferredSource = sourceDescriptor;
			sons.put(sourceDescriptor, data);
			try {
				GenericRecord guiUpdate = createUpdate();
				System.out.print("GUI UPDATE:\n" + guiUpdate);
				return new ProducerRecord<String, Object>("update", sourceDescriptor.getSystemUUID().toString(), guiUpdate);
			} catch (IOException e) {
				System.out.println("failed to generate update");
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	private SourceDescriptor getSourceDescriptor(GenericRecord data) {
		String externalSystemID = data.get("externalSystemID").toString();
		String sourceName = ((GenericRecord) data.get("basicAttributes")).get("sourceName").toString();
		System.out.println("externalSystemID: " + externalSystemID + ", sourceName: " + sourceName);
		for (SourceDescriptor e: sons.keySet()) {
			System.out.println("SourceDescriptor: " + e);
			if (e.getReportsId().equals(externalSystemID) && e.getSensorId().equals(sourceName)) {
				System.out.println("FOUND EXTERNAL ID: " + e + "   SystemID:" + e.getSystemUUID());
				return e;
			}
		}
		throw new RuntimeException("Entity manager received report from a source that doesn't belong to it: "
				+ sourceName + ", " + externalSystemID);
	}
	
	private GenericRecord createUpdate() throws IOException {
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
		if (!stateChange.equals("NONE")) {
			stateChange = "NONE";
		}
		return family;
	}
	
	private GenericRecord createSingleEntityUpdate(GenericRecord latestUpdate, UUID systemUUID) throws IOException {
		return new GenericRecordBuilder(SYSTEM_ENTITY_SCHEMA)
				.set("entityID", systemUUID.toString())
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
					+ "{\"name\": \"entityOffset\",\"type\": \"long\"},"
					+ "{\"name\": \"sourceName\", \"type\": \"string\"}"
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
		if (ENTITY_FAMILY_SCHEMA == null) {
			ENTITY_FAMILY_SCHEMA = parser.parse("{\"type\": \"record\", "
    				+ "\"name\": \"entityFamily\", "
    				+ "\"doc\": \"This is a schema of processed entity with full attributes.\","
    				+ "\"fields\": ["
    					+ "{\"name\": \"entityID\", \"type\": \"string\"},"
						+ "{\"name\": \"stateChanges\",\"type\": \"string\"},"
						+ "{\"name\": \"entityAttributes\", \"type\": \"generalEntityAttributes\"},"
						+ "{\"name\" : \"sons\", \"type\": [{\"type\": \"array\", \"items\": \"systemEntity\"}]}"
					+ "]}");
		}
	}

}
