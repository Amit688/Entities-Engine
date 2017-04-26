package org.z.entities.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EntityManager implements Function<ConsumerRecord<String, Object>, ProducerRecord<Object, Object>> {
	private static Schema BASIC_ATTRIBUTES_SCHEMA = null;
	private static Schema GENERAL_ATTRIBUTES__SCHEMA = null;
	private static Schema SYSTEM_ENTITY_SCHEMA = null;
	private static Schema ENTITY_FAMILY_SCHEMA = null;
	static {
		registerSchemas();
	}
	
	private UUID uuid;
	private Map<SourceDescriptor, GenericRecord> sons;
	private SourceDescriptor preferredSource;
	private String stateChange;
	private Map<UUID, GenericRecord> entities;

	public EntityManager(UUID uuid, String StateChange, List<SourceDescriptor> sources, Map<UUID, GenericRecord> entities) {
		this.uuid = uuid;
		this.stateChange = StateChange;
		preferredSource = null;
		this.entities = entities;
		initSons(sources);
	}

	private void initSons(List<SourceDescriptor> sources) {
		sons = new HashMap<>();
		for (SourceDescriptor source : sources) {
			sons.put(source, entities.get(source.getSystemUUID()));
			entities.remove(source.getSystemUUID());
		}
	}

//	public EntityManager(UUID uuid, String StateChange, Map<SourceDescriptor, GenericRecord> sonsAttributes) {
//		this.uuid = uuid;
//		this.stateChange = StateChange;
//		initSons(sonsAttributes);
//		preferredSource = null;
//	}
//
//	private void initSons(Map<SourceDescriptor, GenericRecord> sonsAttributes) {
//		sons = new HashMap<>();
//		for (Map.Entry<SourceDescriptor, GenericRecord> sonAttributes : sonsAttributes.entrySet()) {
//			sons.put(sonAttributes.getKey(), sonAttributes.getValue());
//		}
//	}

	@Override
	public ProducerRecord<Object, Object> apply(ConsumerRecord<String, Object> record) {
		try {
			System.out.println("processing report for uuid " + uuid + "\nI have " + sons.size() + " sons");
			System.out.println("sons are:");
			for (SourceDescriptor e: sons.keySet())
				System.out.println("system: " + e.getSystemUUID() + ", Reports ID: " + e.getReportsId() + ",  SensorID" + e.getSensorId());
			GenericRecord data = (GenericRecord) record.value();
			this.entities.put(uuid, data);
			SourceDescriptor sourceDescriptor = getSourceDescriptor(data);
			preferredSource = sourceDescriptor;
			GenericRecord sonAttributes = convertGeneralAttributes(data);
			sons.put(sourceDescriptor, sonAttributes);
			try {
				GenericRecord guiUpdate = createUpdate();
				System.out.print("GUI UPDATE:\n" + guiUpdate);
				return new ProducerRecord<>("update", uuid.toString(), guiUpdate);
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
	
	private GenericRecord convertGeneralAttributes(GenericRecord data) {
		EnumSymbol category = convertEnum((GenericData.EnumSymbol) data.get("category"), 
				GENERAL_ATTRIBUTES__SCHEMA.getField("category").schema());
		EnumSymbol nationality = convertEnum((GenericData.EnumSymbol) data.get("nationality"), 
				GENERAL_ATTRIBUTES__SCHEMA.getField("nationality").schema());
		GenericRecordBuilder builder = new GenericRecordBuilder(GENERAL_ATTRIBUTES__SCHEMA)
				.set("basicAttributes", convertBasicAttributes((GenericRecord) data.get("basicAttributes")))
				.set("category", category)
				.set("nationality", nationality);
		copyFields(data, builder, Arrays.asList("speed", "elevation", "course", "pictureURL", "height", "nickname", "externalSystemID"));
		return builder.build();
	}
	
	private GenericRecord convertBasicAttributes(GenericRecord data) {
		GenericRecord coordinateData = (GenericRecord) data.get("coordinate");
		GenericRecordBuilder coordinateBuilder = new GenericRecordBuilder(BASIC_ATTRIBUTES_SCHEMA.getField("coordinate").schema());
		copyFields(coordinateData, coordinateBuilder, Arrays.asList("lat", "long"));
		GenericRecordBuilder builder = new GenericRecordBuilder(BASIC_ATTRIBUTES_SCHEMA)
				.set("coordinate", coordinateBuilder.build());
		copyFields(data, builder, Arrays.asList("isNotTracked", "entityOffset", "sourceName"));
		return builder.build();
	}
	
	private void copyFields(GenericRecord source, GenericRecordBuilder destination, List<String> fields) {
		for (String field : fields) {
			destination.set(field, source.get(field));
		}
	}
	
	private GenericData.EnumSymbol convertEnum(GenericData.EnumSymbol source, Schema targetSchema) {
		return new GenericData.EnumSymbol(targetSchema, source.toString());
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
	
	private static void registerSchemas() {
		Schema.Parser parser = new Schema.Parser();
		BASIC_ATTRIBUTES_SCHEMA = parser.parse("{\"type\": \"record\","
				+ "\"name\": \"basicSystemEntityAttributes\","
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
		GENERAL_ATTRIBUTES__SCHEMA = parser.parse("{\"type\": \"record\", "
				+ "\"name\": \"generalSystemEntityAttributes\","
				+ "\"doc\": \"This is a schema for general entity before acquiring by the system\","
				+ "\"fields\": ["
					+ "{\"name\": \"basicAttributes\",\"type\": \"basicSystemEntityAttributes\"},"
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
		SYSTEM_ENTITY_SCHEMA = parser.parse("{\"type\": \"record\", "
				+ "\"name\": \"systemEntity\","
				+ "\"doc\": \"This is a schema of a single processed entity with all attributes.\","
				+ "\"fields\": ["
					+ "{\"name\": \"entityID\", \"type\": \"string\"}, "
					+ "{\"name\": \"entityAttributes\", \"type\": \"generalSystemEntityAttributes\"}"
				+ "]}");
		ENTITY_FAMILY_SCHEMA = parser.parse("{\"type\": \"record\", "
				+ "\"name\": \"entityFamily\", "
				+ "\"doc\": \"This is a schema of processed entity with full attributes.\","
				+ "\"fields\": ["
					+ "{\"name\": \"entityID\", \"type\": \"string\"},"
					+ "{\"name\": \"stateChanges\", \"type\": \"string\"},"
					+ "{\"name\": \"entityAttributes\", \"type\": \"generalSystemEntityAttributes\"},"
					+ "{\"name\" : \"sons\", \"type\": [{\"type\": \"array\", \"items\": \"systemEntity\"}]}"
				+ "]}");
	}

}
