package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.model.Rule;

public interface MongoDBClientEnricher {
    String getFile(Rule rule);
}
