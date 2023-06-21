package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;

import static com.mongodb.client.model.Filters.eq;

@Slf4j
@Data
@RequiredArgsConstructor
public class MongoDBClientEnricherImpl implements MongoDBClientEnricher {
    private final MongoDatabase mongoDatabase;
    private final String mongoCollection;

    public MongoDBClientEnricherImpl(Config config) {
        var mongoClient = MongoClients.create(config.getString("mongo.connectionString"));
        this.mongoDatabase = mongoClient.getDatabase(config.getString("mongo.database"));
        this.mongoCollection = config.getString("mongo.collection");
    }


    public String read(String conditionField, String conditionValue) {
        var findDocument = getDocument(conditionField, conditionValue);
        if (findDocument != null) {
            return findDocument.toJson();
        }
        return null;
    }


    private Document getDocument(String conditionField, String conditionValue) {
        return mongoDatabase.getCollection(mongoCollection).find(eq(conditionField, conditionValue)).sort(Sorts.descending("_id")).first();
    }
}
