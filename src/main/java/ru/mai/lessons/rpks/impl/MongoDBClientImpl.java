package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClient;

import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;

public class MongoDBClientImpl implements MongoDBClient {
    private final MongoClient mongoClient;
    private final MongoCollection<Document> mongoCollection;

    public MongoDBClientImpl(Config config) {
        mongoClient = MongoClients.create(config.getString("connectionString"));
        String databaseName = config.getString("database");
        String collectionName = config.getString("collection");

        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        mongoCollection = mongoDatabase.getCollection(collectionName);
    }

    @Override
    public Optional<Document> getDocument(String fieldName, String fieldValue) {
        return Optional.ofNullable(
                mongoCollection
                        .find(eq(fieldName, fieldValue))
                        .sort(Sorts.descending("_id"))
                        .first()
        );
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
