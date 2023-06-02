package ru.mai.lessons.rpks.impl;

import com.mongodb.BasicDBObject;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Enrichment;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceEnrichment implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса обогащения
        // Считываем параметры конфигурации для подключения к базе данных
        Config db = config.getConfig("db");
        String url = db.getString("jdbcUrl");
        String user = db.getString("user");
        String password = db.getString("password");
        String driver = db.getString("driver");
        Long updateIntervalSec = config.getLong("application.updateIntervalSec");
        DbReader dbreader = new ReaderDB(url, user, password, driver);

        // Считываем параметры конфигурации для подключения к MongoDB
        String connectionString = config.getString("mongo.connectionString");
        String database = config.getString("mongo.database");
        String collection = config.getString("mongo.collection");
        MongoDB mongoDB = new MongoDB();

        Queue<ArrayList<Enrichment>> queue = new ConcurrentLinkedQueue<>();
        new Thread(() -> {
            try {
                String fieldName = "";
                while(!Objects.equals(fieldName, "exit")) {
                    //Считываем правила из БД
                    Rule[] rules = dbreader.readRulesFromDB();
                    ArrayList<Enrichment> listEnrichments = new ArrayList<>();

                    fieldName = null;
                    String fieldNameEnrichment = null;
                    String fieldValueDefault = null;
                    BasicDBObject criteria = new BasicDBObject();
                    for (Rule rule : rules)
                    {
                        // Поля для поиска документа в MongoDB и записи в Message
                        fieldName = rule.getFieldName();
                        fieldNameEnrichment = rule.getFieldNameEnrichment();
                        String fieldValue = rule.getFieldValue();
                        fieldValueDefault = "\"" + rule.getFieldValueDefault() + "\"";

                        criteria.append(fieldNameEnrichment,fieldValue);
                        String valueDocument = mongoDB.readFromMongoDB(connectionString, database, collection, criteria);
                        if (valueDocument == null) valueDocument = fieldValueDefault;

                        Enrichment enrichment = new Enrichment(fieldName, valueDocument);
                        listEnrichments.add(enrichment);
                    }

                    if(!queue.isEmpty()) queue.remove();
                    queue.add(listEnrichments);

                    Thread.sleep(updateIntervalSec * 1000);
                }
            } catch (InterruptedException ex) {
                log.warn("Interrupted!", ex);
                Thread.currentThread().interrupt();
            }
        }).start();

        //Kafka reader
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            KafkaReader kafkaReader = new KafReader(config, queue);
            kafkaReader.processing();
        });
        executorService.shutdown(); // Завершить после выполнения все задач.
    }
}
