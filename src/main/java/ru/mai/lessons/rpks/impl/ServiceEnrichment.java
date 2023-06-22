package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.Service;

public class ServiceEnrichment implements Service {

    private Config config;
    private KafkaReaderRealization kafkaReader = new KafkaReaderRealization();
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        kafkaReader.setConfig(config);
        kafkaReader.processing();
    }


}
