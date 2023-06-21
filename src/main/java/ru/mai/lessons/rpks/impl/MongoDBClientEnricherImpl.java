package ru.mai.lessons.rpks.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.bson.Document;
import org.bson.types.ObjectId;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;
import java.util.function.Consumer;

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

            String field = null;
            ObjectId maxId = null;
            for (var cl : collection.find(document))
            {
                log.info(cl.toString());
                ObjectId localMax = cl.getObjectId("_id");
                if (maxId == null)
                    maxId = localMax;
                if (localMax.getTimestamp() >= maxId.getTimestamp())
                    field = cl.toJson();
            }

            return Objects.requireNonNullElse(field, rule.getFieldValueDefault());

        }
        catch (Exception e){
            log.info(e.toString());
        }
        return null;
    }
}
