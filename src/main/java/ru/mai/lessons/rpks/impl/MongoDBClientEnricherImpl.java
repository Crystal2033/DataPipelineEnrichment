package ru.mai.lessons.rpks.impl;

import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
@Slf4j
public class MongoDBClientEnricherImpl implements MongoDBClientEnricher, AutoCloseable {
    private final MongoClient mongoClient;
    private final MongoCollection<Document> mongoCollection;

    public MongoDBClientEnricherImpl(String connectionString, String database, String collection) {
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        mongoCollection = mongoDatabase.getCollection(collection);
    }
    @Override
    public Document getEnrichmentRules(String fieldName, String fieldValue) {
        Document searchQuery = new Document();
        searchQuery.put(fieldName, fieldValue);
        return mongoCollection.find(searchQuery).sort(Sorts.descending("_id")).first();
    }

    @Override
    public void close() {
        try {
            mongoClient.close();
        } catch (Exception ex) {
            log.error("Can not close mongo db connection");
        }
    }
}
