package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Data
@Slf4j
public class MongoDBClientEnricherImpl implements MongoDBClientEnricher {
    String collectionName;
    Config config;
    MongoDBClientEnricherImpl(Config appConfig){
        config = appConfig;
    }
    @Override
    public String getFile(Rule rule) {
        try(MongoClient mongoClient = MongoClients.create(config.getString("mongo.connectionString"))) {
            collectionName = config.getString("mongo.collection");
            var database = mongoClient.getDatabase(config.getString("mongo.database"));
            var collection = database.getCollection(collectionName);
            Document document = new Document();
            document.put(rule.getFieldNameEnrichment(), rule.getFieldValue());

            return Objects.requireNonNullElse(
                    Objects.requireNonNull(collection.find(document).sort(Sorts.descending("_id")).first()).toJson(),
                    rule.getFieldValueDefault());

        }
        catch (Exception e){
            log.info(e.toString());
        }
        return null;
    }
}
