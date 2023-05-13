package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceImpl implements Service {
    private static final String DATA_BASE_CONFIG_NAME = "db";
    private static final String KAFKA_CONFIG_NAME = "kafka";
    private static final String APPLICATION_CONFIG_NAME = "application";
    private static final String MONGO_CONFIG_NAME = "mongo";

    private Rule[] rules;
    private DbReader dbReader;
    private RuleProcessor ruleProcessor;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final Object lock = new Object();

    @Override
    public void start(Config config) {
        MongoDBClient mongoDBClient = new MongoDBClientImpl(config.getConfig(MONGO_CONFIG_NAME));
        ruleProcessor = new RuleProcessorImpl(mongoDBClient);

        Config applicationConfig = config.getConfig(APPLICATION_CONFIG_NAME);
        initDBReader(config.getConfig(DATA_BASE_CONFIG_NAME), applicationConfig);

        initScheduledExecutorServiceForRuleUpdate(applicationConfig);
        startKafka(config.getConfig(KAFKA_CONFIG_NAME));
    }

    private void initDBReader(Config dbConfig, Config applicationConfig) {
        dbReader = new DbReaderImpl(dbConfig, applicationConfig.getLong("enrichmentId"));
    }

    private void initScheduledExecutorServiceForRuleUpdate(Config ruleIntervalConfig) {
        String interval = ruleIntervalConfig.getString("updateIntervalSec");
        executorService.scheduleAtFixedRate(
                this::updateRules,
                0,
                Integer.parseInt(interval),
                TimeUnit.SECONDS
        );
    }

    private void startKafka(Config kafkaConfig) {
        Config producerConfig = kafkaConfig.getConfig("producer");
        Config consumerConfig = kafkaConfig.getConfig("consumer");

        KafkaWriter kafkaWriter = KafkaWriterImpl.builder()
                .ruleProcessor(ruleProcessor)
                .rulesGetter(this::getRules)
                .topic(producerConfig.getString("topic"))
                .bootstrapServers(producerConfig.getString("bootstrap.servers"))
                .build();

        KafkaReader kafkaReader = KafkaReaderImpl.builder()
                .kafkaWriter(kafkaWriter)
                .topic(consumerConfig.getString("topic"))
                .groupId(consumerConfig.getString("group.id"))
                .kafkaOffset(consumerConfig.getString("auto.offset.reset"))
                .bootstrapServers(consumerConfig.getString("bootstrap.servers"))
                .build();

        kafkaReader.processing();
        log.debug("Start Service");
    }

    private void updateRules() {
        log.debug("Start reading rules from DB");
        synchronized (lock) {
            rules = dbReader.readRulesFromDB();
        }
        log.debug("End reading rules from DB");
    }

    private Rule[] getRules() {
        synchronized (lock) {
            if (rules == null) {
                rules = dbReader.readRulesFromDB();
            }
            return rules;
        }
    }
}
