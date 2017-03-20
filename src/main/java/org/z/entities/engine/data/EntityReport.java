package org.z.entities.engine.data;

public class EntityReport {
	private Coordinate coordinate;
	private String nationality;
	
	public EntityReport(Coordinate coordinate, String nationality) {
		this.coordinate = coordinate;
		this.nationality = nationality;
	}

	public Coordinate getCoordinate() {
		return coordinate;
	}

	public String getNationality() {
		return nationality;
	}

	@Override
	public String toString() {
		return "EntityReport [coordinate=" + coordinate + ", nationality=" + nationality + "]";
	}
}
