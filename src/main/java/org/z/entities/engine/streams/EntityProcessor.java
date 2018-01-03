package org.z.entities.engine.streams;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger; 
import org.z.entities.engine.SourceDescriptor;
import org.z.entities.engine.utils.Utils;
import org.z.entities.schema.BasicEntityAttributes;
import org.z.entities.schema.Category;
import org.z.entities.schema.DetectionEvent;
import org.z.entities.schema.EntityFamily;
import org.z.entities.schema.GeneralEntityAttributes; 
import org.z.entities.schema.SystemEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class EntityProcessor implements Function<ConsumerRecord<Object, Object>, ProducerRecord<Object, Object>> {

    private UUID uuid;
    private Map<SourceDescriptor, GenericRecord> sons;
    private SourceDescriptor preferredSource;
    private String stateChange;
    private String initialMetadata;

    private static final String defaultMetadata = (String) EntityFamily.SCHEMA$.getField("metadata").defaultVal();

    public final static Logger logger = Logger.getLogger(EntityProcessor.class);
	static {
		Utils.setDebugLevel(logger);
	}

    public EntityProcessor(UUID uuid, Map<SourceDescriptor, GenericRecord> sons, SourceDescriptor preferredSource, String stateChange,
                           String metadata) {
        this.uuid = uuid;
        this.sons = sons;
        this.preferredSource = preferredSource;
        this.stateChange = stateChange;
        this.initialMetadata = metadata;
    }

    public UUID getUuid() {
        return uuid;
    }
    
    public Map<SourceDescriptor, GenericRecord> getSons() {
    	return sons;
    }

    @Override
    public ProducerRecord<Object, Object> apply(ConsumerRecord<Object, Object> record) {  
    	long startTime = System.currentTimeMillis();
        try { 
        	GenericRecord data = (GenericRecord) record.value();  
        	
        	logger.debug("processing report for uuid " + uuid + "\nI have " + sons.size() + " sons");
            logger.debug("sons are:"); 
            SourceDescriptor sourceDescriptor = getSourceDescriptor(data);
            preferredSource = sourceDescriptor;
            GenericRecord sonAttributes = convertGeneralAttributes(data,record.offset());
            sons.put(sourceDescriptor, sonAttributes);
            try {
                ProducerRecord<Object, Object> guiUpdate = generateGuiUpdate();
                logger.debug("GUI UPDATE:\n" + guiUpdate);
                long endTime = System.currentTimeMillis() - startTime;
                logger.debug("END TIME:\n" + endTime);
                return guiUpdate;
            } catch (RuntimeException e) {
            	logger.debug("failed to generate update");
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
        logger.debug("externalSystemID: " + externalSystemID + ", sourceName: " + sourceName);
        for (SourceDescriptor e: sons.keySet()) {
        	logger.debug("SourceDescriptor: " + e);
            if (e.getReportsId().equals(externalSystemID) && e.getSensorId().equals(sourceName)) {
            	logger.debug("FOUND EXTERNAL ID: " + e + "   SystemID:" + e.getSystemUUID());
                return e;
            }
        }
        throw new RuntimeException("Entity manager received report from a source that doesn't belong to it: "
                + sourceName + ", " + externalSystemID);
    }

    private GenericRecord convertGeneralAttributes(GenericRecord data, long lastStateOffset) {
       // GenericData.EnumSymbol category = convertEnum((GenericData.EnumSymbol) data.get("category"),
       // 		//GeneralEntityAttributes.SCHEMA$.getField("category").schema());
       // 		Category.SCHEMA$);
       // GenericData.EnumSymbol nationality = convertEnum((GenericData.EnumSymbol) data.get("nationality"),
       // 		GeneralEntityAttributes.SCHEMA$.getField("nationality").schema());
        GenericRecordBuilder builder = new GenericRecordBuilder(GeneralEntityAttributes.SCHEMA$)
                .set("basicAttributes", convertBasicAttributes((GenericRecord) data.get("basicAttributes")))
                //.set("category", category)
               // .set("nationality", nationality);
               .set("category", data.get("category"))
               .set("nationality", data.get("nationality"))
               .set("lastStateOffset", lastStateOffset);
        copyFields(data, builder, Arrays.asList("speed", "elevation", "course", "pictureURL", "height", "nickname", "externalSystemID", "metadata"));
        return builder.build();
    }

    private GenericRecord convertBasicAttributes(GenericRecord data) {
        GenericRecord coordinateData = (GenericRecord) data.get("coordinate");
        GenericRecordBuilder coordinateBuilder = new GenericRecordBuilder(BasicEntityAttributes.SCHEMA$.getField("coordinate").schema());
        copyFields(coordinateData, coordinateBuilder, Arrays.asList("lat", "long"));
        GenericRecordBuilder builder = new GenericRecordBuilder(BasicEntityAttributes.SCHEMA$)
                .set("coordinate", coordinateBuilder.build());
        copyFields(data, builder, Arrays.asList("isNotTracked", "sourceName"));
        return builder.build();
    }

    private void copyFields(GenericRecord source, GenericRecordBuilder destination, List<String> fields) {
        for (String field : fields) {
            destination.set(field, source.get(field));
        }
    }
 
    public ProducerRecord<Object, Object> generateGuiUpdate() {
        return new ProducerRecord<>("update", uuid.toString(), getCurrentState());
    }

    public GenericRecord getCurrentState() {
        List<GenericRecord> sonsRecords = new ArrayList<>();
        for (SourceDescriptor sonKey : sons.keySet()) {
            logger.debug(sonKey);
            logger.debug(sons.get(sonKey));
            sonsRecords.add(createSingleEntityUpdate(sons.get(sonKey), sonKey.getSystemUUID()));
        }

        String metadataForThisUpdate = initialMetadata;
        if (metadataForThisUpdate.equals(defaultMetadata)) {
            metadataForThisUpdate = (String) sons.get(preferredSource).get("metadata");
        }

        GenericRecord family = new GenericRecordBuilder(EntityFamily.SCHEMA$)
                .set("entityID", uuid.toString())
                .set("entityAttributes", sons.get(preferredSource))
                .set("sons", sonsRecords)
                .set("stateChanges", stateChange)
                .set("metadata", metadataForThisUpdate)
                .build();

        if (!stateChange.equals("NONE")) {
            stateChange = "NONE";
        }
        if (!initialMetadata.equals(defaultMetadata)) {
            initialMetadata = defaultMetadata;
        }
        return family;
    }

    private GenericRecord createSingleEntityUpdate(GenericRecord latestUpdate, UUID systemUUID) {
        return new GenericRecordBuilder(SystemEntity.SCHEMA$)
                .set("entityID", systemUUID.toString())
                .set("entityAttributes", latestUpdate) 
                .build();
    } 
}
