package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import com.typesafe.config.Config;
import static com.mongodb.client.model.Filters.eq;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class MongoImpl implements MongoDBClientEnricher {
    @NonNull
    Config config;

    String nameCollection;

    public String findDocument(String fieldNameEnrichment, String fieldValue) {
        String documentToReturn;
        nameCollection = config.getString("mongo.collection");
        try (var mongoClient = MongoClients.create(config.getString("mongo.connectionString"))) {
            log.debug("Create and get Database");
            MongoDatabase mongoDatabase = mongoClient.getDatabase(config.getString("mongo.database"));
            log.debug("Get Collection");
            MongoCollection<Document> documentMongoCollection = mongoDatabase.getCollection(nameCollection);

            Optional<Document> findDocument = Optional.ofNullable(documentMongoCollection.find(eq(fieldNameEnrichment, fieldValue)).sort(new Document("_id", -1)).first());
            if (findDocument.isPresent()) {
                documentToReturn = findDocument.get().toJson();
            } else {
                documentToReturn = "";
            }
        }
        return documentToReturn;
    }
}
