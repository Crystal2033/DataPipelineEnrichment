package ru.mai.lessons.rpks;


import org.bson.Document;

public interface MongoDBClientEnricher {
    public Document read(String fieldName, String fieldValue);
}
