/**
 * 
 */
package org.z.entities.engine;

import static org.junit.Assert.*; 
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList; 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID; 

import org.apache.avro.generic.GenericRecord; 
import org.apache.kafka.clients.producer.ProducerRecord;  
import org.junit.BeforeClass;
import org.junit.Test; 
import org.z.entities.engine.streams.EntityProcessor; 

public class EntityProcessorTest {

	private static EntityProcessor entityProcessor1Son;
	private static EntityProcessor entityProcessor2Son; 
 
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		UUID uuid = UUID.randomUUID();
		String sourceName = "source0";
		String externalSystemID = "externalSystemID0";
		SourceDescriptor sourceDescriptor1 = new SourceDescriptor(sourceName,"externalSystemID0",1,uuid);
		Map<SourceDescriptor, GenericRecord> sons = new HashMap<>(); 
		sons.put(sourceDescriptor1, TestUtils.getGenericRecord("source0","externalSystemID0",""));
		List<SourceDescriptor> sourceDescriptors = new ArrayList<SourceDescriptor>(); 
		sourceDescriptors.add(sourceDescriptor1); 

		entityProcessor1Son = new EntityProcessor(uuid, sons, sourceDescriptors.iterator().next(), "NONE", ""); 		
		
		sourceName = "source1";
		externalSystemID = "externalSystemID1";
		uuid = UUID.randomUUID();
		SourceDescriptor sourceDescriptor2 = new SourceDescriptor(sourceName,externalSystemID,1,uuid);  
		sons.put(sourceDescriptor2, TestUtils.getGenericRecord(sourceName, externalSystemID,""));
		sourceDescriptors.add(sourceDescriptor2); 
		entityProcessor2Son = new EntityProcessor(uuid, sons, sourceDescriptors.iterator().next(), "NONE", ""); 
	}

	/**
	 *  Entity with one son and the processor is getting a new update
	 *  the expected result is state update with the new values
	 */
	@Test(expected = RuntimeException.class)
	public void testReportIdDoesntBelongToProcessor1Son() {
		try {
			GenericRecord data = TestUtils.getGenericRecord("source3", "externalSystemID3","");
			entityProcessor1Son.apply(data );
		} catch (IOException | RestClientException e) {		 
			fail();
		} 
	}
	
	/**
	 *  Entity with one son and the processor is getting a new update
	 *  that with a different report and source name than it has
	 *  the expected result exception
	 * 	
	 */
	@Test
	public void testReportIdBelongToProcessor1Son() {
		GenericRecord data = null;
		try {
			data = TestUtils.getGenericRecord("source0", "externalSystemID0","");
		} catch (IOException | RestClientException e) {		 
			fail();
		}
		ProducerRecord<Object, Object> producerRecord = entityProcessor1Son.apply(data );

		assertNotNull(producerRecord);
	}

	/**
	 *  Entity with 2 sons and the processor is getting a new update
	 *  the expected result is state update with the new values
	 */
	@Test(expected = RuntimeException.class)
	public void testReportIdDoesntBelongToProcessor2Son() {
		try {
			GenericRecord data = TestUtils.getGenericRecord("source3", "externalSystemID3","");
			entityProcessor2Son.apply(data );
		} catch (IOException | RestClientException e) {		 
			fail();
		} 
	}
	
	/**
	 *  Entity with 2 sons and the processor is getting a new update
	 *  that with a different report and source name than it has
	 *  the expected result exception
	 * 	
	 */
	@Test
	public void testReportIdBelongToProcessor2Son() {
		GenericRecord data = null; 
		try {
			data = TestUtils.getGenericRecord("source1", "externalSystemID1","");
		} catch (IOException | RestClientException e) {		 
			fail();
		}
		ProducerRecord<Object, Object> producerRecord = entityProcessor2Son.apply(data );

		assertNotNull(producerRecord);
	}  
}
