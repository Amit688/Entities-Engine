package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;

import java.util.Collection; 

public class MergeValidationService {
    public boolean validateMerge(Collection<GenericRecord> entitiesToMerge) {
        return true;
    }
}
