package ru.mai.lessons.rpks.services;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.dispatchers.EnrichmentDispatcher;
import ru.mai.lessons.rpks.kafka.impl.KafkaReaderImpl;
import ru.mai.lessons.rpks.mongo.impl.MongoEnrichmentClient;
import ru.mai.lessons.rpks.processors.impl.EnrichmentProcessor;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;
import ru.mai.lessons.rpks.repository.impl.DataBaseReader;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;
import ru.mai.lessons.rpks.services.interfaces.Service;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceEnrichment implements Service {

    private static final String KAFKA_NAME = "kafka";
    private static final String TOPIC_NAME_PATH = "topic.name";
    private Config outerConfig;

    @Override
    public void start(Config config) {
        outerConfig = config;
        try (DataBaseReader dataBaseReader = initExistingDBReader(outerConfig.getConfig("db"))) {
            connectToDBAndWork(dataBaseReader);
        }
    }

    private void startKafkaReader(EnrichmentDispatcher dispatcherKafka) {

        Config config = outerConfig.getConfig(KAFKA_NAME).getConfig("consumer");

        KafkaReaderImpl kafkaReader = KafkaReaderImpl.builder()
                .topic(config.getConfig("enrichment").getString(TOPIC_NAME_PATH))
                .autoOffsetReset(config.getString(("auto.offset.reset")))
                .bootstrapServers(config.getString("bootstrap.servers"))
                .groupId(config.getString("group.id"))
                .dispatcherKafka(dispatcherKafka)
                .build();

        kafkaReader.processing();
    }

    private DataBaseReader initExistingDBReader(Config configDB) {
        return DataBaseReader.builder()
                .url(configDB.getString("jdbcUrl"))
                .userName(configDB.getString("user"))
                .password(configDB.getString("password"))
                .driver(configDB.getString("driver"))
                .additionalDBConfig(configDB.getConfig("additional_info"))
                .build();
    }

    private void connectToDBAndWork(DataBaseReader dataBaseReader) {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        connectAndRun(dataBaseReader, scheduledExecutorService);
        scheduledExecutorService.shutdown();
    }

    private void connectAndRun(DataBaseReader dataBaseReader, ScheduledExecutorService executorService) {
        try {
            if (dataBaseReader.connectToDataBase()) {
                RulesUpdaterThread rulesDBUpdaterThread = new RulesUpdaterThread(dataBaseReader, outerConfig);

                MongoEnrichmentClient mongoEnrichmentClient = new MongoEnrichmentClient(outerConfig);
                mongoEnrichmentClient.connectToMongo();
                mongoEnrichmentClient.initCollection();

                RuleProcessor ruleProcessor = new EnrichmentProcessor(mongoEnrichmentClient);

                EnrichmentDispatcher enrichmentDispatcher = new EnrichmentDispatcher(outerConfig, rulesDBUpdaterThread, ruleProcessor);

                long delayTimeInSec = outerConfig.getConfig("application").getLong("updateIntervalSec");
                executorService.scheduleWithFixedDelay(rulesDBUpdaterThread, 0, delayTimeInSec, TimeUnit.SECONDS);

                startKafkaReader(enrichmentDispatcher);
            } else {
                log.error("There is a problem with connection to database.");
            }
        } catch (SQLException exc) {
            log.error("There is a problem with getConnection from Hikari.");
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            log.debug("All threads are done.");
            executorService.shutdownNow();
        }
    }
}
