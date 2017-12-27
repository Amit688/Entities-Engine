/**
 * 
 */
package org.z.entities.engine;

import static org.junit.Assert.*; 
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException; 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID; 

import org.apache.avro.generic.GenericRecord; 
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;  
import org.junit.BeforeClass;
import org.junit.Test; 
import org.z.entities.engine.streams.EntityProcessor;  

public class EntityProcessorTest {

	private static EntityProcessor entityProcessorOneSon;
	private static EntityProcessor entityProcessorTwoSon;  
	
 
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		UUID uuid = UUID.randomUUID();
		Map<SourceDescriptor, GenericRecord> oneSon = new HashMap<>(); 
		oneSon.put(new SourceDescriptor("source0","externalSystemID0",1,0,UUID.randomUUID()),  
				                        TestUtils.getGenericRecord("source0","externalSystemID0",""));
		
		entityProcessorOneSon = new EntityProcessor(uuid, oneSon, oneSon.entrySet().iterator().next().getKey(), "NONE", ""); 
		
		Map<SourceDescriptor, GenericRecord> twoSon = new HashMap<>();
		
		twoSon.put(new SourceDescriptor("source0","externalSystemID0",1,0,UUID.randomUUID()), 
                TestUtils.getGenericRecord("source0","externalSystemID0",""));
		twoSon.put(new SourceDescriptor("source1","externalSystemID1",1,0,UUID.randomUUID()), 
                TestUtils.getGenericRecord("source1","externalSystemID1",""));
		
		entityProcessorTwoSon = new EntityProcessor(uuid, twoSon, twoSon.entrySet().iterator().next().getKey(), "NONE", ""); 
	}

	/**
	 *  Entity with one son and the processor is getting a new update
	 *  that with a different report and source name than it has
	 *  the expected result exception
	 */
	//@Test(expected = RuntimeException.class)
	public void testReportIdDoesntBelongToProcessor1Son() {
		try {
			GenericRecord data = TestUtils.getGenericRecord("source3", "externalSystemID3","");
			ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<Object, Object>("externalSystemID3", 0, 0, data, data);
			entityProcessorOneSon.apply(consumerRecord);
		} catch (IOException | RestClientException e) {		 
			fail();
		} 
	}
	
	/**
	 *  Entity with one son and the processor is getting a new update 
	 *  the expected result the state is updated with the new details 
	 * 	
	 */
	//@Test
	public void testReportIdBelongToProcessor1Son() {
		GenericRecord data = null;
		try {
			data = TestUtils.getGenericRecord("source0", "externalSystemID0","junit");
		} catch (IOException | RestClientException e) {		 
			fail();
		}
		ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<Object, Object>("externalSystemID0", 0, 0, data, data);
		ProducerRecord<Object, Object> producerRecord = entityProcessorOneSon.apply(consumerRecord);

		assertNotNull(producerRecord);
		
		GenericRecord record = (GenericRecord)producerRecord.value();
		assertEquals("junit", record.get("metadata").toString());
		
		List<GenericRecord> sons = getSons(record);
		GenericRecord entityAttributes = (GenericRecord) sons.get(0).get("entityAttributes");
		assertEquals("externalSystemID0", entityAttributes.get("externalSystemID").toString());
		GenericRecord basicAttributes = (GenericRecord) entityAttributes.get("basicAttributes");
		assertEquals("source0", basicAttributes.get("sourceName").toString()); 
	}

	/**
	 *  Entity with 2 sons and the processor is getting a new update
	 *  that with a different report and source name than it has
	 *  the expected result is exception
	 */
	//@Test(expected = RuntimeException.class)
	public void testReportIdDoesntBelongToProcessor2Son() {
		try {
			GenericRecord data = TestUtils.getGenericRecord("source3", "externalSystemID3","");
			ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<Object, Object>("externalSystemID3", 0, 0, data, data);
			entityProcessorTwoSon.apply(consumerRecord);
		} catch (IOException | RestClientException e) {		 
			fail();
		} 
	}
	
	/**
	 *  Entity with 2 sons and the processor is getting a new update
	 *  the expected result the state is updated with the new details 	 * 	
	 */
	//@Test
	public void testReportIdBelongToProcessor2Son() {
		GenericRecord data = null; 
		try {
			data = TestUtils.getGenericRecord("source1", "externalSystemID1","junit");
		} catch (IOException | RestClientException e) {		 
			fail();
		}
		ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<Object, Object>("externalSystemID1", 0, 0, data, data);
		ProducerRecord<Object, Object> producerRecord = entityProcessorTwoSon.apply(consumerRecord);
		
		GenericRecord record = (GenericRecord)producerRecord.value();
		assertEquals("junit", record.get("metadata").toString());

		List<GenericRecord> sons = getSons(record);
		GenericRecord entityAttributes = (GenericRecord) sons.get(0).get("entityAttributes");
		assertEquals("externalSystemID0", entityAttributes.get("externalSystemID").toString());
		GenericRecord basicAttributes = (GenericRecord) entityAttributes.get("basicAttributes");
		assertEquals("source0", basicAttributes.get("sourceName").toString()); 
		
		entityAttributes = (GenericRecord) sons.get(1).get("entityAttributes");
		assertEquals("externalSystemID1", entityAttributes.get("externalSystemID").toString());
		basicAttributes = (GenericRecord) entityAttributes.get("basicAttributes");
		assertEquals("source1", basicAttributes.get("sourceName").toString()); 
	}  
	
	@SuppressWarnings("unchecked")
	private List<GenericRecord> getSons(GenericRecord entity) {
		return (List<GenericRecord>) entity.get("sons");
	}

}
