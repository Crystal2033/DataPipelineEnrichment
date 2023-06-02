package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;

//https://hub.docker.com/_/mongo
//docker run --name some-mongo -d -p 27017:27017 mongo:latest

@Slf4j
@RequiredArgsConstructor
public class MongoClientImpl {
    @NonNull
    Config config;
    ObjectMapper mapper;
    public void mongo(Message message, Rule rule) {
        try (var mongoClient = MongoClients.create(config.getConfig("mongo").getString("connectionString"))) {
            log.info("Create and get Database");

            MongoDatabase mongoDatabase = mongoClient.getDatabase(config.getConfig("mongo").getString("database"));

            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(config.getConfig("mongo").getString("collection"));


            ArrayList<Document> documentArrayList = mongoCollection.find().into(new ArrayList<>());
            log.info("GET DOCS");
            try {
                mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(message.getValue());
                Document document = new Document();
                document.append(rule.getFieldNameEnrichment(), rule.getFieldValue());
                FindIterable<Document> elements = mongoCollection.find(document).sort(new Document("_id", -1));
                if (!elements.cursor().hasNext()) {
                    String d = rule.getFieldValueDefault();

//
//                    ObjectNode jsonNodeTemp = (ObjectNode) mapper.readTree(d);
                    ((ObjectNode) jsonNode).put(rule.getFieldName(), d);
//                    ((ObjectNode) jsonNode).set(rule.getFieldName(), rule.getFieldValueDefault());
                } else {
                    String d = Objects.requireNonNull(elements.first()).toJson();
                    JsonNode jsonNodeTemp = mapper.readTree(d);
                    ((ObjectNode) jsonNode).set(rule.getFieldName(), jsonNodeTemp);
                }
                message.setValue(jsonNode.toString());

            } catch (JsonProcessingException e) {
                log.error(e.getMessage());
            } catch (NullPointerException e) {
                log.error(e.getMessage());
            }
//            Document harryPotterPartOne = new Document()
//                    .append("name", "Harry Potter and the Philosopher's Stone")
//                    .append("year", 2001)
//                    .append("director", "Chris Colambus")
//                    .append("available", true)
//                    .append("actors", Arrays.asList("Redcliff", "Watson", "Grint"));
//            log.info("Document created: {}", harryPotterPartOne.toJson());
//
//            log.info("Get Collection");
//            MongoCollection<Document> fantasy = mongoDatabase.getCollection("fantasy");
//            log.info("Collection: {}", fantasy);
//
//            log.info("Insert document into collection");
//            fantasy.insertOne(harryPotterPartOne);
//
//            Document harryPotterCursedChild = new Document()
//                    .append("name", "Harry Potter and cursed child")
//                    .append("available", false)
//                    .append("comments", "waiting");
//
//            Document harryPotterCursedChildPart2 = new Document()
//                    .append("name", "Harry Potter and cursed child part 2")
//                    .append("available", false)
//                    .append("comments", "waiting");
//
//            log.info("Insert many documents into collection");
//            fantasy.insertMany(Arrays.asList(harryPotterCursedChild, harryPotterCursedChildPart2));
//
//            log.info("Find document by name");
//            Optional<Document> findDocument = Optional.ofNullable(fantasy.find(eq("name", "Harry Potter and the Philosopher's Stone")).first());
//            findDocument.ifPresent(doc -> log.info("Find document: {}", doc.toJson()));
//
//            findDocument.ifPresent(doc -> {
//                log.info("Document name: {}", doc.getString("name"));
//            });
//
//            log.info("Find document by other name");
//            Optional<Document> notFoundDocument = Optional.ofNullable(fantasy.find(eq("name", "Harry Potter")).first());
//            notFoundDocument.ifPresentOrElse(doc -> log.info("Find document: {}", doc.toJson()), () -> log.info("Document not found"));
//
//            log.info("Find document by regex name");
//            ArrayList<Document> documentsByRegex = fantasy.find(regex("name", "Harry Potter")).into(new ArrayList<>());
//            log.info("Found documents: {}", documentsByRegex.size());
//            documentsByRegex.forEach(doc -> log.info("Found: {}", doc.toJson()));
//
//            log.info("Delete document");
//            fantasy.deleteMany(eq("name", "Harry Potter and cursed child part 2"));
//
//            log.info("Find document after delete");
//            Optional<Document> documentAfterDelete = Optional.ofNullable(fantasy.find(eq("name", "Harry Potter and cursed child part 2")).first());
//            documentAfterDelete.ifPresentOrElse(doc -> log.info("Find document: {}", doc.toJson()), () -> log.info("Document not found"));
//
//            log.info("Delete collections");
//            mongoDatabase.getCollection("triller").drop();
//            mongoDatabase.getCollection("comedy").drop();
//            mongoDatabase.getCollection("fantasy").drop();
//
//            if (!mongoDatabase.listCollectionNames().iterator().hasNext()) {
//                log.info("Database empty");
//            }
        }
    }
}