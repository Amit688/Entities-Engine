package org.z.entities.engine;

import akka.stream.javadsl.SourceQueueWithComplete; 
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.generic.GenericRecord; 
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.z.entities.engine.utils.Utils;
import org.z.entities.schema.DetectionEvent;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * @author assafsh
 * Aug 2017
 * 
 * Each interface will have it's own MailRoom instance
 * that will hold a map with all the producer records in 
 * Map<String,Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>>> dataMap 
 * 
 * exteranlSystemID | Pair<ConcurrentLinkedQueue<GenericRecord>,SourceQueue<GenericRecord>>
 *
 */
public class MailRoom implements java.util.function.Consumer<GenericRecord> {
	
    private String sourceName;
	private ConcurrentMap<String, BlockingQueue<GenericRecord>> reportsQueues;
	private SourceQueueWithComplete<GenericRecord> creationQueue;

    final static public Logger logger = Logger.getLogger(MailRoom.class);
	static {
		Utils.setDebugLevel(logger);
	}
	
	public MailRoom(String sourceName) {
        this.sourceName = sourceName;
		this.reportsQueues = new ConcurrentHashMap<>();
		this.creationQueue = null;
	} 
	
	public MailRoom() {
		
	} 
	
	public void setCreationQueue(SourceQueueWithComplete<GenericRecord> queue) {
		this.creationQueue = queue;
	}

	@Override
	public void accept(GenericRecord record) {
 		logger.debug("MailRoom <"+sourceName+"> accept Message "+record);
		String externalSystemId = record.get("externalSystemID").toString();
        BlockingQueue<GenericRecord> reportsQueue = reportsQueues.get(externalSystemId);
        if (reportsQueue != null) {
            logger.debug("Existing externalSystemID");
            reportsQueues.get(externalSystemId).offer(record);
        } else {
            logger.debug("New externalSystemID");
            BlockingQueue<GenericRecord> queue = new LinkedBlockingQueue<>();
            queue.add(record);
            reportsQueues.put(externalSystemId, queue);
            String metadata = (String) record.get("metadata");
            publishToCreationTopic(externalSystemId, metadata);
        }
	}

	private void publishToCreationTopic(String externalSystemId, String metadata) {
        try {
            creationQueue.offer(getGenericRecordForCreation(externalSystemId, metadata));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
    }

	protected GenericRecord getGenericRecordForCreation(String externalSystemID, String metadata)
            throws IOException, RestClientException {
		DetectionEvent detectionEvent = DetectionEvent.newBuilder()
		        .setSourceName(sourceName)
		        .setExternalSystemID(externalSystemID)
		        .setDataOffset(3333L)
                .setMetadata(metadata)
		        .build();

		return detectionEvent;
	}
	 
    public BlockingQueue<GenericRecord> getReportsQueue(String externalSystemId) {
        return reportsQueues.get(externalSystemId);
    }
 
}
