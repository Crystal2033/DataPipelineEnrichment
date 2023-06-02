package ru.mai.lessons.rpks.configs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.configs.interfaces.ConfigReader;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.load(); // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
