package org.z.entities.engine;

import org.apache.avro.generic.GenericRecord;

public class EntitiesEvent {
	private EntitiesEvent.Type type;
	private GenericRecord data;
	
	public EntitiesEvent(EntitiesEvent.Type type, GenericRecord data) {
		this.type = type;
		this.data = data;
	}

	public EntitiesEvent.Type getType() {
		return type;
	}

	public GenericRecord getData() {
		return data;
	}
	
	public static enum Type {
		CREATE,
		MERGE,
		SPLIT
	}
}