package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClients;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import org.bson.Document;

@Slf4j
public class MyMongoDBClientEnricher implements MongoDBClientEnricher {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;


    public MyMongoDBClientEnricher(Config config) {
        this.connectionString = config.getString("connectionString");
        this.databaseName = config.getString("database");
        this.collectionName = config.getString("collection");
        log.info("ConnectionString: " + connectionString);
        log.info("DatabaseName: " + databaseName);
        log.info("CollectionName: " + collectionName);
        log.info("MongoDBClientEnricher created");
    }
    @Override
    public Document read(String fieldName, String fieldValue) {
        try (var mongoClient = MongoClients.create(connectionString)) {
            return mongoClient.getDatabase(databaseName)
                    .getCollection(collectionName)
                    .find(new Document(fieldName, fieldValue))
                    .sort(new Document("_id", -1)).first();
        }
    }
}
