package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jooq.*;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.jooq.impl.DSL.*;

@Slf4j
@RequiredArgsConstructor
public class Db implements DbReader {
    DataSource dataSource;
    @NonNull
    Config config;
    @NonNull
    String dbDriver;
    Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    DataSource getDataSource(){
        if (dataSource == null){
            log.info("No DataSource is available. We will create a new one.");
            createDataSource();
        }
        return dataSource;
    }
    void createDataSource(){
        try {
            Class.forName(dbDriver);
            HikariConfig hikariConfig = getHikariConfig();
            log.info("Configuration is ready.");
            log.info("Creating the HiKariDataSource and assigning it as the global");
            dataSource = new HikariDataSource(hikariConfig);
        }
        catch (Exception e){
            log.error("caught datasource exception");
        }
    }
    HikariConfig getHikariConfig() {
        log.info("Creating the config with HikariConfig");
        HikariConfig hikaConfig = null;
        try {
            hikaConfig = new HikariConfig();
            Class.forName(dbDriver);
            hikaConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
            //username
            hikaConfig.setUsername(config.getString("db.user"));
            //password
            hikaConfig.setPassword(config.getString("db.password"));
            //driver class name
            hikaConfig.setDriverClassName(dbDriver);
            return hikaConfig;
        } catch (Exception e) {
            log.error("caught hika config exception");
        }

        return hikaConfig;
    }

    @Override
    public Rule[] readRulesFromDB() {
        try (Connection connection = getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = config.getString("db.table");
            int numberOfRows = context.fetchCount(context.selectFrom(tableName));
            Rule[] ruleArray = new Rule[numberOfRows];
            var result = context.select().from(tableName).fetch();
            ArrayList<Rule> array = new ArrayList<>();
            result.forEach(res -> {
                try {
                    Long enrichmentId = (Long)res.getValue("enrichment_id");
                    Long ruleId = (Long)res.getValue("rule_id");
                    String fieldName = (String)res.getValue("field_name");
                    String fieldNameEnrichment = (String)res.getValue("field_name_enrichment");
                    String fieldValue = (String)res.getValue("field_value");
                    String fieldValueDefault = (String)res.getValue("field_value_default");
                    Rule rule = new Rule(enrichmentId, ruleId, fieldName, fieldNameEnrichment, fieldValue, fieldValueDefault);
                    array.add(rule);
                } catch (Exception e) {
                    log.error("caight rule exception");
                }
            });
            array.toArray(ruleArray);
            return ruleArray;
        }
        catch (SQLException e) {
            log.error("DB rules error!");
            throw new IllegalStateException("DB rules error");
        }
        catch (Exception e) {
            log.error("CAUGHT FETCH EX");
            return new Rule[0];
        }

    }
}
