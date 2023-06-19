package ru.mai.lessons.rpks;

import org.bson.Document;

public interface MongoDBClientEnricher {
    public String read(String conditionField, String conditionValue);
}
