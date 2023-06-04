package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.*;
import com.typesafe.config.Config;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class MongoClientImpl {
    @NonNull
    Config config;
    ObjectMapper mapper;
    public void mongo(Message message, Rule rule) {
        try (var mongoClient = MongoClients.create(config.getString("mongo.connectionString"))) {
            log.info("Create and get Database");
            MongoDatabase mongoDatabase = mongoClient.getDatabase(config.getString("mongo.database"));
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(config.getString("mongo.collection"));
            mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(message.getValue());
                Document document = new Document();
                document.append(rule.getFieldNameEnrichment(), rule.getFieldValue());
                FindIterable<Document> mongoDocuments = mongoCollection.find(document).sort(new Document("_id", -1));
                //if no elements then put
                if (!mongoDocuments.cursor().hasNext()) {
                    String field = rule.getFieldValueDefault();
                    ((ObjectNode) jsonNode).put(rule.getFieldName(), field);
                } else {
                    String field = Objects.requireNonNull(mongoDocuments.first()).toJson();
                    JsonNode jsonNodeTemp = mapper.readTree(field);
                    ((ObjectNode) jsonNode).set(rule.getFieldName(), jsonNodeTemp);
                }
                message.setValue(jsonNode.toString());

            } catch (JsonProcessingException e) {
                log.error("caught json processing exception");
            } catch (NullPointerException e) {
                log.error("caught null pointer exception");
            }
        }
    }
}