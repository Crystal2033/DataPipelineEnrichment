package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class MyKafkaReader implements KafkaReader {
    private Rule[] rules;
    private final Config config;
    private MyDbReader db;
    private long updateIntervalSec;
    private final KafkaConsumer<String, String> consumer;

    private final MyRuleProcessor processor;

    public MyKafkaReader(Config config) {
        this.config = config;
        Config configKafkaConsumer = config.getConfig("kafka").getConfig("consumer");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configKafkaConsumer.getConfig("bootstrap").getString("servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configKafkaConsumer.getConfig("auto").getConfig("offset").getString("reset"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, configKafkaConsumer.getConfig("group").getString("id"));
        consumer = new KafkaConsumer<>(properties);
        updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");
        processor = new MyRuleProcessor(config);
    }

    @Override
    public void processing() {
        MyKafkaWriter kafkaWriter = new MyKafkaWriter(config);
        db = new MyDbReader(config.getConfig("db"));
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
                for (Rule r :
                        rules) {
                    log.debug(r.toString());
                }
            }
        };
        Timer timer = new Timer(true);
        timer.schedule(task, 0, 1000 * updateIntervalSec);

        try {
            consumer.subscribe(Collections.singleton(config.getConfig("kafka").getConfig("consumer").getString("topic")));
            ConsumerRecords<String, String> records;
            Message message;
            while (!Thread.interrupted()) {
                records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        message = new Message(consumerRecord.value());
                        message = processor.processing(message, rules);
                        kafkaWriter.processing(message);
                    }
                }

            }
            log.info("exit");
        } catch (InterruptException e) {
            log.error(e.getMessage());
        }

    }
}
