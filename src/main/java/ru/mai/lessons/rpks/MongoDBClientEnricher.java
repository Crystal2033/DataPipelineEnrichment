package ru.mai.lessons.rpks;

import org.bson.Document;

public interface MongoDBClientEnricher {



    Document getEnrichmentRules(String fieldName, String fieldValue);
}
