package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;

class EntitiesEvent {
	static enum Type {
		CREATE
	}
	
	public EntitiesEvent.Type type;
	public GenericRecord data;
	
	public EntitiesEvent(EntitiesEvent.Type type, GenericRecord data) {
		this.type = type;
		this.data = data;
	}
}