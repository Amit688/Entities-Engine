package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;

class EntitiesEvent {
	static enum Type {
		CREATE,
		MERGE,
		SPLIT
	}
	
	public EntitiesEvent.Type type;
	public GenericRecord data;
	
	public EntitiesEvent(EntitiesEvent.Type type, GenericRecord data) {
		this.type = type;
		this.data = data;
	}
}