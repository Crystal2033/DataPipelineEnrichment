package ru.mai.lessons.rpks;

import org.bson.Document;

public interface MongoDBClientEnricher {
    Document read(String name, String value);
}
