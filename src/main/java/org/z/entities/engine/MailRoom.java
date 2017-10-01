package org.z.entities.engine;

import akka.stream.javadsl.SourceQueueWithComplete;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.z.entities.engine.utils.Utils;
import org.z.entities.schema.DetectionEvent;

import java.io.Closeable;
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
public class MailRoom implements java.util.function.Consumer<GenericRecord>,Closeable {
    private static boolean testing = Main.testing;

    private String sourceName;
    private KafkaProducer<Object, Object> producer;
	private ConcurrentMap<String, BlockingQueue<GenericRecord>> reportsQueues;
	private SourceQueueWithComplete<GenericRecord> creationQueue;

    final static public Logger logger = Logger.getLogger(MailRoom.class);
	static {
		Utils.setDebugLevel(logger);
	}
	
	public MailRoom(String sourceName, KafkaProducer<Object, Object> producer) {
        this.sourceName = sourceName;
        this.producer = producer;
		this.reportsQueues = new ConcurrentHashMap<>();
		this.creationQueue = null;
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
            publishToCreationTopic(externalSystemId);
        }
	}

	private void publishToCreationTopic(String externalSystemId) {
        try {
            creationQueue.offer(getGenericRecordForCreation(externalSystemId));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
    }

	private GenericRecord getGenericRecordForCreation(String externalSystemID) throws IOException, RestClientException {

		Schema creationSchema;
		if(testing) {
			SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient(); 
			schemaRegistry.register("DetectionEvent", DetectionEvent.SCHEMA$);  
			int id = schemaRegistry.getLatestSchemaMetadata("DetectionEvent").getId();	
			creationSchema = schemaRegistry.getByID(id);
		}
		else {
			creationSchema = getSchema("DetectionEvent");
		}

		GenericRecord creationRecord = new GenericRecordBuilder(creationSchema)
		.set("sourceName", sourceName)
		.set("externalSystemID",externalSystemID)
		.set("dataOffset",3333L)
		.build();

		return creationRecord;
	}

	private Schema getSchema(String name) throws IOException, RestClientException {

		String schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS");
		String schemaRegistryIdentity = System.getenv("SCHEMA_REGISTRY_IDENTITY");		
		SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, Integer.parseInt(schemaRegistryIdentity));
		name = "org.z.entities.schema."+name;
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}

    public BlockingQueue<GenericRecord> getReportsQueue(String externalSystemId) {
        return reportsQueues.get(externalSystemId);
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
