package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

public class DataBaseReader implements DbReader {
    private final Config settings;
    private HikariDataSource dataSource;
    private final HikariConfig config = new HikariConfig();
    private final long id;

    public DataBaseReader(Config conf, long id){
        settings = conf.getConfig("db");
        this.id = id;
        connectToDb();
    }

    public void connectToDb(){
        config.setJdbcUrl(settings.getString("jdbcUrl"));
        config.setUsername(settings.getString("user"));
        config.setPassword(settings.getString("password"));
        config.setDriverClassName(settings.getString("driver"));
        dataSource = new HikariDataSource(config);
    }

    @Override
    public Rule[] readRulesFromDB() throws SQLException {
        try (Connection connection = dataSource.getConnection()){
            DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
            return dsl.select().from("enrichment_rules").
                    where(String.format("enrichment_rules.enrichment_id = %s", id)).fetchInto(Rule.class).toArray(Rule[]::new);
        }
    }
}