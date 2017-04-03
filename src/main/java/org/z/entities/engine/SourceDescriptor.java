package org.z.entities.engine;

import java.util.UUID;

public class SourceDescriptor {
	private UUID systemUUID;
	private String sensorId;
	private String reportsId;
	
	public SourceDescriptor(String sensorId, String reportsId, UUID uuid) {
		this.sensorId = sensorId;
		this.reportsId = reportsId;
		this.systemUUID = uuid;
	}

	public String getSensorId() { return sensorId;}

	public String getReportsId() { return reportsId; }

	public UUID getSystemUUID() { return systemUUID; }
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((reportsId == null) ? 0 : reportsId.hashCode());
		result = prime * result + ((sensorId == null) ? 0 : sensorId.hashCode());
		result = prime * result + ((systemUUID == null) ? 0 : systemUUID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		SourceDescriptor other = (SourceDescriptor) obj;
		if (reportsId == null) {
			if (other.reportsId != null) {
				return false;
			}
		} else if (!reportsId.equals(other.reportsId)) {
			return false;
		}
		if (sensorId == null) {
			if (other.sensorId != null) {
				return false;
			}
		} else if (!sensorId.equals(other.sensorId)) {
			return false;
		}
		if (systemUUID == null) {
			if (other.systemUUID != null) {
				return false;
			}
		} else if (!systemUUID.equals(other.systemUUID)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "TopicDescriptor [sensorId=" + sensorId + ", reportsId=" + reportsId + ", systemUUID=" + systemUUID.toString() + "]";
	}
}
