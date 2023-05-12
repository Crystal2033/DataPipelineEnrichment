package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.ConfigReader;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
        return ConfigFactory.load();
    }
}
