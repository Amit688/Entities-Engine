package org.z.entities.engine;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscription;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer; 
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.log4j.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaComponentsFactory {
	private ActorSystem system;
	private SchemaRegistryClient schemaRegistry;
	private String kafkaUrl;
	private final boolean sharingSources;
	private final boolean sharingSinks;
	private ActorRef consumerActor;
	private KafkaProducer<Object, Object> kafkaProducer = null;
 
    final static private Logger logger = Logger.getLogger(KafkaComponentsFactory.class);
 
	static {
		org.z.entities.engine.utils.Utils.setDebugLevel(logger);
	}

	
	public KafkaComponentsFactory(ActorSystem system, SchemaRegistryClient schemaRegistry, String kafkaUrl,
								  boolean sharingSources, boolean sharingSinks) {
		this.system = system;
		this.schemaRegistry = schemaRegistry;
		this.kafkaUrl = kafkaUrl;
		this.sharingSources = sharingSources;
		this.sharingSinks = sharingSinks;
		if (this.sharingSources) {
			this.consumerActor = system.actorOf((KafkaConsumerActor.props(createConsumerSettings())));
		}
 
		ProducerSettings<Object, Object> producerSettings = createProducerSettings();
		this.kafkaProducer = producerSettings.createKafkaProducer();	 
	}

	/**
	 * Creates a source that filters its messages by a reportId
	 *
	 * @param topic
	 * @param reportsId
	 * @return
	 
	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic, String reportsId,long dataOffset) {
		return getSource(topic,dataOffset).filter(record -> filterByReportsId(record, reportsId));
	}
	*/
	
	private boolean filterByReportsId(ConsumerRecord<Object, Object> incomingUpdate, String reportsId) {
		GenericRecord data = (GenericRecord) incomingUpdate.value();
		return data.get("externalSystemID").toString().equals(reportsId);
	} 
	
	/**
	 * Creates a source from a source descriptor
	 *
	 * @param descriptor
	 * @return
	 
	 * NOT IN USE
	 
	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(SourceDescriptor descriptor) {
		return getSource(descriptor.getSensorId(), descriptor.getReportsId(),descriptor.getDataOffset());
	}
	*/

	public Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic){
		int numOfPartitions = getNumOfPartitionForTopic(topic);
		Set<TopicPartition> set = new HashSet<>();
		for(int i = 0; i < numOfPartitions; i++) {
			set.add(new TopicPartition(topic, i));
		}
		if (sharingSources) {
 
			return Consumer.plainExternalSource(consumerActor, Subscriptions.assignment(set));
		} else { 
			return Consumer.plainSource(createConsumerSettings(),
					(Subscription) Subscriptions.assignment(set));
		}
	} 
 
	
	public Source<ConsumerRecord<Object, Object>, ?> getSource(SourceDescriptor descriptor) { 
		TopicPartition topicPartition = new TopicPartition(descriptor.getSensorId(), descriptor.getPartition());
 
		logger.debug("Create source for new entity (no filter - no offset) - "+descriptor); 
		String reportsId = descriptor.getReportsId();
		return Consumer.plainSource(createConsumerSettings(),
//				(Subscription) Subscriptions.assignmentWithOffset(topicPartition,descriptor.getDataOffset()));
		(Subscription) Subscriptions.assignment(topicPartition));
		 	//.filter(record -> filterByReportsId(record, reportsId));
	}

	/*
	private Source<ConsumerRecord<Object, Object>, Consumer.Control> getSource(String topic, long offset) {
		
		if (sharingSources) {
			return Consumer.plainExternalSource(consumerActor,
					Subscriptions.assignmentWithOffset(getTopicPartition(topic), offset));
		} else { 
			return Consumer.plainSource(createConsumerSettings(),
					(Subscription) Subscriptions.assignmentWithOffset(getTopicPartition(topic), offset));
		}
	}
 */

	private ConsumerSettings<Object, Object> createConsumerSettings() {
		KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer(schemaRegistry);
		
		Map<String,String> map = new ConcurrentHashMap<String, String>();				
		map.put("schema.registry.url", "http://fake-url");
		map.put("max.schemas.per.subject", String.valueOf(Integer.MAX_VALUE)); 
		keyDeserializer.configure(map, true);
		
		return ConsumerSettings.create(system, keyDeserializer,
				new KafkaAvroDeserializer(schemaRegistry))
        		.withBootstrapServers(kafkaUrl)
        		.withGroupId(UUID.randomUUID().toString())
        		.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	private TopicPartition getTopicPartition(String topic) {
		return new TopicPartition(topic, 0);
	}

	public Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> getSink() {
		ProducerSettings<Object, Object> producerSettings = createProducerSettings();
		if (sharingSinks){
			return Producer.plainSink(producerSettings, kafkaProducer);
		} else {
			return Producer.plainSink(producerSettings);
		}
	}

	private ProducerSettings<Object, Object> createProducerSettings() {
		KafkaAvroSerializer keySerializer = new KafkaAvroSerializer(schemaRegistry);
		
		Map<String,String> map = new ConcurrentHashMap<String, String>();		
		map.put("schema.registry.url", "http://fake-url");
		map.put("max.schemas.per.subject", String.valueOf(Integer.MAX_VALUE));		
		keySerializer.configure(map, true);
		
		return ProducerSettings
                    .create(system, keySerializer, new KafkaAvroSerializer(schemaRegistry))
                    .withBootstrapServers(kafkaUrl);
	}

	public KafkaProducer<Object, Object> getKafkaProducer() {
		if (sharingSinks) {
			return kafkaProducer;
		} else {
			return createProducerSettings().createKafkaProducer();
		}
	}
	
	public int getPartitionByKey (String topic,String key) {
		KafkaProducer<Object, Object> producer = kafkaProducer;		

		int numPartitions = producer.partitionsFor(topic).size();
		try(KafkaAvroSerializer keySerializer = new KafkaAvroSerializer(schemaRegistry)) {
			Map<String,String> map = new ConcurrentHashMap<String, String>();		
			map.put("schema.registry.url", "http://fake-url");
			map.put("max.schemas.per.subject", String.valueOf(Integer.MAX_VALUE));		
			keySerializer.configure(map, true);
			
			byte[] keyBytes	 = keySerializer.serialize(topic, key);
			keySerializer.close();
			return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;					
		}	
	} 
	
	public int getPartitionByKey(GenericRecord record) {
        GenericRecord attributes = (GenericRecord) record.get("entityAttributes");
        String key = attributes.get("externalSystemID").toString();
        String topic = ((GenericRecord) attributes.get("basicAttributes")).get("sourceName").toString();  		
		return getPartitionByKey (topic,key);	
	}
	
	public int getNumOfPartitionForTopic(String topic) {
		KafkaProducer<Object, Object> producer = kafkaProducer;	
		return producer.partitionsFor(topic).size(); 
	}
}
