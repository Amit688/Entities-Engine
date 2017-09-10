package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;

import java.util.UUID;

/**
 * A class to wrap GenericRecord that contains a son's data
 */
public class SonAccessor {
    public static SourceDescriptor getSourceDescriptor(GenericRecord sonData) {
        UUID uuid = UUID.fromString(sonData.get("entityID").toString());
        GenericRecord attributes = (GenericRecord) sonData.get("entityAttributes");
        String externalSystemID = attributes.get("externalSystemID").toString();
        String sourceName = ((GenericRecord) attributes.get("basicAttributes")).get("sourceName").toString();
        return new SourceDescriptor(sourceName, externalSystemID,0, uuid);
    }

    public static GenericRecord getAttributes(GenericRecord sonData) {
        return (GenericRecord) sonData.get("generalEntityAttributes");
    }
}
