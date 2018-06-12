package org.z.entities.engine.sagas;

import org.apache.avro.generic.GenericRecord;

public class SplitValidationService {
    public boolean validate(GenericRecord lastState) {
        return true;
    }
}
