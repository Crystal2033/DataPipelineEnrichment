package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

public class MyKafkaWriter implements KafkaWriter {
    private final Config config;
    private final KafkaProducer<String, String> producer;

    public MyKafkaWriter(Config configIn) {
        config = configIn;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConfig("kafka").getConfig("producer").getConfig("bootstrap").getString("servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void processing(Message message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                config.getConfig("kafka").getConfig("producer").getString("topic"),
                message.getValue()
        );

        producer.send(producerRecord);
    }
}