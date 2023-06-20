package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServiceEnrichment implements Service {
    private Rule[] rules;
    private DbReader dbReader;

    private final Object locker = new Object();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private void loadRules() {
        synchronized (locker) {
            rules = Arrays.stream(dbReader.readRulesFromDB()).sorted(Comparator.comparing(Rule::getRuleId))
                    .toArray(Rule[]::new);
        }

    }

    private Rule[] getRules() {
        Rule[] resRules;
        synchronized (locker) {
            resRules = rules.clone();
        }
        return resRules;
    }

    @Override
    public void start(Config config) {

        Config app = config.getConfig("application");
        dbReader = new DbReaderImpl(config.getConfig("db"), app.getString("enrichmentId"));
        loadRules();
        executor.scheduleAtFixedRate(
                this::loadRules,
                0,
                Integer.parseInt(app.getString("updateIntervalSec")),
                TimeUnit.SECONDS
        );

        Config mongo = config.getConfig("mongo");

        MongoDBClientEnricher mongoDBClientEnricher = new MongoDBClientEnricherImpl(
                mongo.getString("connectionString"), mongo.getString("database"),
                mongo.getString("collection"));


        RuleProcessor ruleProcessor = new RuleProcessorImpl(mongoDBClientEnricher);

        Config kafka = config.getConfig("kafka");
        Config producer = kafka.getConfig("producer");

        KafkaWriter kafkaWriter = new KafkaWriterImpl(producer.getString("topic"),
                producer.getString("bootstrap.servers"));

        Config consumer = kafka.getConfig("consumer");

        KafkaReader kafkaReader = new KafkaReaderImpl(kafkaWriter, this::getRules, ruleProcessor, consumer.getString("topic"),
                consumer.getString("auto.offset.reset"),
                consumer.getString("group.id"),
                consumer.getString("bootstrap.servers"));

        kafkaReader.processing();

    }
}
