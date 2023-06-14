package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.typesafe.config.Config;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.Sorts;

public class MongoDBClient implements MongoDBClientEnricher {

    private final MongoClient mongoClient;
    private final MongoCollection<Document> documents;
    public MongoDBClient(Config config) {
        mongoClient = MongoClients.create(config.getString("connectionString"));
        documents = mongoClient.getDatabase(config.getString("database")).getCollection(config.getString("collection"));
    }
    @Override
    public Document read(String name, String value){
        return documents.find(eq(name, value)).sort(Sorts.descending("_id")).first();
    }

}
