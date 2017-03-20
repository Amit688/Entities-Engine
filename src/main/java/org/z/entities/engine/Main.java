package org.z.entities.engine;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by Amit on 20/03/2017.
 */
public class Main {

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("Sys");
        final Materializer materializer = ActorMaterializer.create(system);

        ConsumerSettings<String, String> consumerSettings = ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers("localhost:9092")
                .withGroupId("group1")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Source<ConsumerRecord<String, String>, Consumer.Control> source =
                Consumer.plainSource(consumerSettings,
                        Subscriptions.assignment(new TopicPartition("test", 0)));

        EntitiesSupervisor supervisor = new EntitiesSupervisor(materializer, source);
    }
}
