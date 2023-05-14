package ru.mai.lessons.rpks;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.impl.ConfigReaderImpl;
import ru.mai.lessons.rpks.impl.ServiceImpl;

@Slf4j
public class ServiceEnrichmentMain {
    public static void main(String[] args) {
        log.info("Start service Enrichment");
        ConfigReader configReader = new ConfigReaderImpl();
        Service service = new ServiceImpl(); // ваша реализация service
        service.start(configReader.loadConfig());
        log.info("Terminate service Enrichment");
    }
}