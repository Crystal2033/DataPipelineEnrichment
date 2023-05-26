package ru.mai.lessons.rpks;

import org.bson.Document;

import java.io.Closeable;

public interface MongoDBClientEnricher extends Closeable {
    Document getDoc(String fieldName, String fieldValue);
}
