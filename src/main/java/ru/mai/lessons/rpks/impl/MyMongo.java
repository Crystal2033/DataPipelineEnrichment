package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.*;
import com.typesafe.config.Config;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Slf4j
@AllArgsConstructor
public class MyMongo implements MongoDBClientEnricher {
    MongoClient mongoClient;
    MongoDatabase database;
    Config config;
    ObjectMapper mapper;
    MongoCollection<Document> todoCollection;

    public MyMongo(Config config) {
        Config mongoConfig = config.getConfig("mongo");
        mapper = new ObjectMapper();
        this.config = config;
        mongoClient = MongoClients.create(mongoConfig.getString("connectionString"));
        database = mongoClient.getDatabase(mongoConfig.getString("database"));
        todoCollection = database.getCollection(config.getConfig("mongo").getString("collection"));
    }


    public void checkAndReplace(Message message, Rule rule) {
        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());
            Document document = new Document();
            document.append(rule.getFieldNameEnrichment(), rule.getFieldValue());
            FindIterable<Document> elements = todoCollection.find(document).sort(new Document("_id", -1));
            if (!elements.cursor().hasNext()) {
                jsonNode = ((ObjectNode) jsonNode).put(rule.getFieldName(), rule.getFieldValueDefault());
            } else {
                String d = Objects.requireNonNull(elements.first()).toJson();
                JsonNode node1 = mapper.readTree(d);
                jsonNode = ((ObjectNode) jsonNode).set(rule.getFieldName(), node1);
            }
            message.setValue(jsonNode.toString());

        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        } catch (NullPointerException e) {
            log.error(e.getMessage());
        }

    }
}
