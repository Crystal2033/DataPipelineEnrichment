package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

public class DbReaderImpl implements DbReader {
    private final HikariDataSource hikariDataSource;

    public DbReaderImpl(Config dbConfig) {
        String url = dbConfig.getString("jdbcUrl");
        String user = dbConfig.getString("user");
        String password = dbConfig.getString("password");
        String driver = dbConfig.getString("driver");

        HikariConfig dataBaseConfig = new HikariConfig();
        dataBaseConfig.setJdbcUrl(url);
        dataBaseConfig.setUsername(user);
        dataBaseConfig.setPassword(password);
        dataBaseConfig.setDriverClassName(driver);

        this.hikariDataSource = new HikariDataSource(dataBaseConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        return new Rule[0];
    }
}
