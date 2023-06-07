package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
@Slf4j
public class ServiceEnrichment implements Service {
    Db db;
    Rule[] rules;
    int updateIntervalSec;
    @Override
    public void start(Config config) {
        rules = new Rule[1];
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        updateIntervalSec = config.getInt("application.updateIntervalSec");
        String dbDriver = config.getConfig("db").getString("driver");
        db = new Db(config, dbDriver);
        rules = db.readRulesFromDB();
        MongoClientImpl mongoClient = new MongoClientImpl(config);
        String reader = config.getString("kafka.consumer.bootstrap.servers");
        String writer = config.getString("kafka.producer.bootstrap.servers");
        String topicIn = config.getString("kafka.topicIn");
        String topicOut = config.getString("kafka.topicOut");
        KafkaReaderImpl kafkaReader = new KafkaReaderImpl(topicIn, topicOut, reader, writer, rules, config, mongoClient);
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB();
                kafkaReader.setRules(rules);
            }
        };
        Timer timer = new Timer(true);
        timer.schedule(task, 0, 1000L * updateIntervalSec);
        executorService.execute(kafkaReader::processing);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                task.cancel();
                timer.cancel();
            }
        });

    }
}
