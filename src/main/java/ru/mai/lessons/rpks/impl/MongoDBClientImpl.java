package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;

import static com.mongodb.client.model.Filters.eq;

public class MongoDBClientImpl implements MongoDBClientEnricher {
    private final MongoClient client;
    private final MongoCollection<Document> documents;

    public MongoDBClientImpl(Config config) {
        client = MongoClients.create(config.getString("connectionString"));
        documents = client.getDatabase(config.getString("database")).getCollection(config.getString("collection"));
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public Document getDoc(String name, String value) {
        return documents.find(eq(name, value)).sort(Sorts.descending("_id")).first();
    }
}
