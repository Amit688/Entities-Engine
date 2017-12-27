package org.z.entities.engine;

import java.util.UUID;

public class SourceDescriptor {
	private UUID systemUUID;
	private String sensorId;
	private String reportsId;
	private int partition;
	private long dataOffset;
	
	public SourceDescriptor(String sensorId, String reportsId, long dataOffset, int partition, UUID uuid) {
		this.sensorId = sensorId;
		this.reportsId = reportsId;
		this.dataOffset = dataOffset;
		this.partition = partition;
		this.systemUUID = uuid;		
	}

	public String getSensorId() { return sensorId;}

	public String getReportsId() { return reportsId; }

	public UUID getSystemUUID() { return systemUUID; }
	
	public long getDataOffset() { return dataOffset; }
	
	public int getPartition() { return partition; }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((reportsId == null) ? 0 : reportsId.hashCode());
		result = prime * result + ((sensorId == null) ? 0 : sensorId.hashCode()); 
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
		return true;
	}

	@Override
	public String toString() {
		return "SourceDescriptor [systemUUID=" + systemUUID + ", sensorId="
				+ sensorId + ", reportsId=" + reportsId + ", partition="
				+ partition + ", dataOffset=" + dataOffset + "]";
	} 
}
