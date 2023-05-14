package ru.mai.lessons.rpks;

import org.bson.Document;

import java.io.Closeable;
import java.util.Optional;

public interface MongoDBClient extends Closeable {
    Optional<Document> getDocument(String fieldName, String fieldValue);
}
