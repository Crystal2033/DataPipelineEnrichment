package ru.mai.lessons.rpks.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.jooq.tools.json.JSONObject;
import ru.mai.lessons.rpks.model.Enrichment;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.descending;

@Slf4j
@Data
@RequiredArgsConstructor
public class MongoDB {
    public String readFromMongoDB(String connectionString, String database, String collection, BasicDBObject criteria)
    {
        try (var mongoClient = MongoClients.create(connectionString))
        {
            String valueEnrichment = null;
            MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

            Document findDocument = mongoCollection.find(criteria).sort(descending("_id")).first();
            if (findDocument != null) valueEnrichment = findDocument.toJson();

            int index = 1;
            if (valueEnrichment != null){
                // Переносим _id в конец документа
                index = valueEnrichment.indexOf(',');
                String before = valueEnrichment.substring(1, index);
                String after = valueEnrichment.substring(index+2, valueEnrichment.length() - 1);
                valueEnrichment = "{" + after + ", " + before + "}";
            }
            return valueEnrichment;
        }
        catch (Exception ex){
            log.error("Error mongoClient", ex);
            return null;
        }
    }
}
