package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.MongoDBClientEnricher;

public class MongoDBClientEnricherImpl implements MongoDBClientEnricher {

    MongoDBClientEnricherImpl(Config config){

    }
    @Override
    public String getFile(String file) {
        return null;
    }
}
