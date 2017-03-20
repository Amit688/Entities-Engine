package org.z.entities.engine;

import java.util.function.Function;

import org.z.entities.engine.data.EntityReport;

public class EntityManager implements Function<EntityReport, EntityReport> {

	@Override
	public EntityReport apply(EntityReport arg0) {
		System.out.println("processing report");
		return arg0;
	}

}
