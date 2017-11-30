package org.z.entities.engine;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import joptsimple.internal.Strings;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord; 
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper; 
import org.z.entities.schema.BasicEntityAttributes;
import org.z.entities.schema.Category;
import org.z.entities.schema.Coordinate;
import org.z.entities.schema.GeneralEntityAttributes;
import org.z.entities.schema.Nationality;

public class TestUtils {
	
	public static GenericRecord getGenericRecord(String sourceName, String externalSystemID,String metadata) throws IOException, RestClientException {
		String json = getJsonGenericRecord(sourceName, externalSystemID,metadata);
		EntityReport entityReport = getEntityReportFromJson(json); 
		if(Strings.isNullOrEmpty(entityReport.getMetadata())) {
			metadata = (String) GeneralEntityAttributes.SCHEMA$.getField("metadata").defaultVal();
		}

		Coordinate coordinate = Coordinate.newBuilder().setLat(entityReport.getLat())
				.setLong$(entityReport.getXlong())
				.build();

		BasicEntityAttributes basicEntity = BasicEntityAttributes.newBuilder().setCoordinate(coordinate)
				.setEntityOffset(0)
				.setIsNotTracked(false)
				.setSourceName(entityReport.getSource_name())
				.build();

		GeneralEntityAttributes entity = GeneralEntityAttributes.newBuilder()
				.setCategory(Category.valueOf(entityReport.getCategory()))
				.setCourse(entityReport.getCourse())
				.setElevation(entityReport.getElevation())
				.setExternalSystemID(entityReport.getId())
				.setHeight(entityReport.getHeight())
				.setNationality(Nationality.valueOf(entityReport.getNationality().toUpperCase()))
				.setNickname(entityReport.getNickname())
				.setPictureURL(entityReport.getPicture_url())
				.setSpeed(entityReport.getSpeed())
				.setBasicAttributes(basicEntity)
				.setMetadata(metadata)
				.build();
		
		return entity;
	}
	
	private static String getJsonGenericRecord(String sourceName, String externalSystemID,String metadata) {
		return   "{\"id\":\""+externalSystemID+"\"," 
				+"\"metadata\":\""+metadata+"\"," 		
				+"\"lat\": 222," 
				+"\"xlong\": 333," 
				+"\"source_name\":\""+sourceName+"\"," 
				+"\"category\":\"boat\","
				+"\"speed\":\"444\", "
				+"\"course\":\"5.55\", "
				+"\"elevation\":\"7.8\"," 
				+"\"nationality\":\"USA\"," 
				+"\"picture_url\":\"URL\", "
				+"\"height\":\"44\","
				+"\"nickname\":\"mick\"," 
				+" \"timestamp\":\""+Long.toString(System.currentTimeMillis())+"\"  }";
	} 

	private static EntityReport getEntityReportFromJson(String json) throws JsonParseException, JsonMappingException, IOException {
		EntityReport entityReport = new ObjectMapper().readValue(json, EntityReport.class);  
		return entityReport;
	}
	
	public static Schema getDummySchema() {
		return new org.apache.avro.Schema.Parser().parse(
				"{\"type\": \"record\"," +
					 "\"name\": \"DummySchema\","+ 
					 "\"namespace\" : \"org.z.entities.schema\","+
					 "\"doc\": \"This is a schema for unit test only\","+  
					 "\"fields\":"+
					 "[{\"name\": \"entityID\", \"type\":\"string\", \"default\":\"\"}]}"); 
	}
}
