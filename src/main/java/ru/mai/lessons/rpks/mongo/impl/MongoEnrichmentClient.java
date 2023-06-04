package ru.mai.lessons.rpks.mongo.impl;

import com.mongodb.MongoClientException;
import com.mongodb.client.*;
import com.typesafe.config.Config;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;

@RequiredArgsConstructor
public class MongoEnrichmentClient {

    private static final String MONGO_STR = "mongo";
    private final Config connectionConfig;
    private MongoClient mongoClient;

    private MongoCollection<Document> mongoCollection;

    public void connectToMongo() {
        mongoClient = MongoClients.create(connectionConfig.getConfig(MONGO_STR).getString("connectionString"));
    }

    public void initCollection() throws IllegalArgumentException, MongoClientException {
        MongoDatabase mongoDB = Optional.ofNullable(mongoClient).orElseThrow(
                        () -> new MongoClientException("Mongo client is empty. Try to reconnect"))
                .getDatabase(connectionConfig.getConfig(MONGO_STR).getString("database"));

        mongoCollection = mongoDB.getCollection(connectionConfig.getConfig(MONGO_STR).getString("collection"));
    }

    public Optional<Document> getDocumentByRule(Rule rule) {
        FindIterable<Document> docs = Optional.ofNullable(mongoCollection).orElseThrow(
                        () -> new MongoClientException("Collection was not found. Document is unachievable."))
                .find(eq(rule.getFieldNameEnrichment(), rule.getFieldValue()));

        Optional<Document> actualDocument = Optional.ofNullable(docs.first());
        for (Document doc : docs) {
            if (actualDocument.isPresent() && doc.get("_id").toString().compareTo(actualDocument.get().get("_id").toString()) > 0) {
                actualDocument = Optional.of(doc);
            }
        }
        return actualDocument;
    }
}
