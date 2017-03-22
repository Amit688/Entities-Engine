package org.z.entities.engine;

import java.util.UUID;
import java.util.function.Function;

import org.z.entities.engine.data.EntityReport;

public class EntityManager implements Function<EntityReport, EntityReport> {
	private UUID uuid;
	
	public EntityManager(UUID uuid) {
		this.uuid = uuid;
	}

	@Override
	public EntityReport apply(EntityReport arg0) {
		System.out.println("processing report for uuid " + uuid);
		return arg0;
	}

}
