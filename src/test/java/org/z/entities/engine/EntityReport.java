package org.z.entities.engine;

public class EntityReport {
	
	private String id;
	private Double lat;
	private Double xlong;
	private String source_name;
	private String category;
	private Double speed;
	private Double course;
	private Double elevation;
	private String nationality;
	private String picture_url;
	private Double height;
	private String nickname;
	private Double timestamp;
	private String metadata;
	
	public String getId() {
		return id;
	}
	public Double getLat() {
		return lat;
	}
	public Double getXlong() {
		return xlong;
	}
	public String getSource_name() {
		return source_name;
	}
	public String getCategory() {
		return category;
	}
	public Double getSpeed() {
		return speed;
	}
	public Double getCourse() {
		return course;
	}
	public Double getElevation() {
		return elevation;
	}
	public String getNationality() {
		return nationality;
	}
	public String getPicture_url() {
		return picture_url;
	}
	public Double getHeight() {
		return height;
	}
	public String getNickname() {
		return nickname;
	}
	public Double getTimestamp() {
		return timestamp;
	}
	public String getMetadata() {
		return metadata;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	public void setXlong(Double xlong) {
		this.xlong = xlong;
	}
	public void setSource_name(String source_name) {
		this.source_name = source_name;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public void setSpeed(Double speed) {
		this.speed = speed;
	}
	public void setCourse(Double course) {
		this.course = course;
	}
	public void setElevation(Double elevation) {
		this.elevation = elevation;
	}
	public void setNationality(String nationality) {
		this.nationality = nationality;
	}
	public void setPicture_url(String picture_url) {
		this.picture_url = picture_url;
	}
	public void setHeight(Double height) {
		this.height = height;
	}
	public void setNickname(String nickname) {
		this.nickname = nickname;
	}
	public void setTimestamp(Double timestamp) {
		this.timestamp = timestamp;
	}
	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((category == null) ? 0 : category.hashCode());
		result = prime * result + ((course == null) ? 0 : course.hashCode());
		result = prime * result
				+ ((elevation == null) ? 0 : elevation.hashCode());
		result = prime * result + ((height == null) ? 0 : height.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((lat == null) ? 0 : lat.hashCode());
		result = prime * result
				+ ((nationality == null) ? 0 : nationality.hashCode());
		result = prime * result
				+ ((nickname == null) ? 0 : nickname.hashCode());
		result = prime * result
				+ ((picture_url == null) ? 0 : picture_url.hashCode());
		result = prime * result
				+ ((source_name == null) ? 0 : source_name.hashCode());
		result = prime * result + ((speed == null) ? 0 : speed.hashCode());
		result = prime * result
				+ ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((xlong == null) ? 0 : xlong.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntityReport other = (EntityReport) obj;
		if (category == null) {
			if (other.category != null)
				return false;
		} else if (!category.equals(other.category))
			return false;
		if (course == null) {
			if (other.course != null)
				return false;
		} else if (!course.equals(other.course))
			return false;
		if (elevation == null) {
			if (other.elevation != null)
				return false;
		} else if (!elevation.equals(other.elevation))
			return false;
		if (height == null) {
			if (other.height != null)
				return false;
		} else if (!height.equals(other.height))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (lat == null) {
			if (other.lat != null)
				return false;
		} else if (!lat.equals(other.lat))
			return false;
		if (nationality == null) {
			if (other.nationality != null)
				return false;
		} else if (!nationality.equals(other.nationality))
			return false;
		if (nickname == null) {
			if (other.nickname != null)
				return false;
		} else if (!nickname.equals(other.nickname))
			return false;
		if (picture_url == null) {
			if (other.picture_url != null)
				return false;
		} else if (!picture_url.equals(other.picture_url))
			return false;
		if (source_name == null) {
			if (other.source_name != null)
				return false;
		} else if (!source_name.equals(other.source_name))
			return false;
		if (speed == null) {
			if (other.speed != null)
				return false;
		} else if (!speed.equals(other.speed))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (xlong == null) {
			if (other.xlong != null)
				return false;
		} else if (!xlong.equals(other.xlong))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "EntityReport [id=" + id + ", lat=" + lat + ", xlong=" + xlong
				+ ", source_name=" + source_name + ", category=" + category
				+ ", speed=" + speed + ", course=" + course + ", elevation="
				+ elevation + ", nationality=" + nationality + ", picture_url="
				+ picture_url + ", height=" + height + ", nickname=" + nickname
				+ ", timestamp=" + timestamp + "]";
	} 
}
