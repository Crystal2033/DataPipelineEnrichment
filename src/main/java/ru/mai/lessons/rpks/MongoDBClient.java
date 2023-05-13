package ru.mai.lessons.rpks;

import org.bson.Document;

import java.util.Optional;

public interface MongoDBClient {
    Optional<Document> getDocument(String fieldName, String fieldValue);
}
