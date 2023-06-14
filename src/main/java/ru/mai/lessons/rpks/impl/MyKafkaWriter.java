package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import java.util.Properties;

@Slf4j
public class MyKafkaWriter implements KafkaWriter {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    public MyKafkaWriter(Config config) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrap.servers"));
        topic = config.getString("topic");
        producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
    }
    @Override
    public void processing(Message message) {
        log.debug("WRITER -- Sending message {} to topic {}", message.getValue(), topic);
        producer.send(new ProducerRecord<>(topic, message.getValue()));
    }
}
