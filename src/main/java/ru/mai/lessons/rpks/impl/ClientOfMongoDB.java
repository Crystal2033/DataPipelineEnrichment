package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.impl.settings.MongoDBSettings;

@Slf4j
@Builder
public class ClientOfMongoDB implements MongoDBClientEnricher {
    MongoDBSettings mongoDBSettings;
}
