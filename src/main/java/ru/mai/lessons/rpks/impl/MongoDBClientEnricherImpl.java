package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import ru.mai.lessons.rpks.MongoDBClientEnricher;

public class MongoDBClientEnricherImpl implements MongoDBClientEnricher {
    MongoDBClientEnricherImpl(Config config){
        MongoClient mongoClient = MongoClients.create(config.getString("mongo.connectionString"));
        MongoDatabase database = mongoClient.getDatabase("mongo.database");

    }
    @Override
    public String getFile(String file) {
        return null;
    }
}
