package org.z.entities.engine;

public class TopicDescriptor {
	private String sensorId;
	private String reportsId;
	
	public TopicDescriptor(String sensorId, String reportsId) {
		this.sensorId = sensorId;
		this.reportsId = reportsId;
	}

	public String getSensorId() {
		return sensorId;
	}

	public String getReportsId() {
		return reportsId;
	}

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
		TopicDescriptor other = (TopicDescriptor) obj;
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
		return "TopicDescriptor [sensorId=" + sensorId + ", reportsId=" + reportsId + "]";
	}
}
