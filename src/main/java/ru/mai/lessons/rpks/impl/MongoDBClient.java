package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;

public class MongoDBClient implements MongoDBClientEnricher{

    private final MongoClient mongoClient;
    private final MongoCollection<Document> mongoCollection;
    public MongoDBClient(Config config) {
        mongoClient = MongoClients.create(config.getString("mongo.connectionString"));
        MongoDatabase mongoDatabase = mongoClient.getDatabase(config.getString("mongo.database"));
        mongoCollection = mongoDatabase.getCollection(config.getString("mongo.collection"));

    }

    public Document getEnrichmentRules(String key, String value) {
        Document doc = new Document();
        doc.put(key, value);
        return mongoCollection.find(doc).sort(Sorts.descending("_id")).first();
    }
}
