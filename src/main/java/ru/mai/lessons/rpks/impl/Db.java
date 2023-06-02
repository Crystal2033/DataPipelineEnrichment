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
            String driver = config.getString("db.driver");
            Class.forName(driver);
            HikariConfig hikariConfig = getHikariConfig();
            log.info("Configuration is ready.");
            log.info("Creating the HiKariDataSource and assigning it as the global");
            dataSource = new HikariDataSource(hikariConfig);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    HikariConfig getHikariConfig() {
        log.info("Creating the config with HikariConfig");
        String driver = config.getString("db.driver");
        HikariConfig hikaConfig = null;
        try {
            hikaConfig = new HikariConfig();
            Class.forName(driver);
            hikaConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
            //username
            hikaConfig.setUsername(config.getString("db.user"));
            //password
            hikaConfig.setPassword(config.getString("db.password"));
            //driver class name
            hikaConfig.setDriverClassName(driver);
            return hikaConfig;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hikaConfig;
    }

    @Override
    public Rule[] readRulesFromDB() {
        try (Connection connection = getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = "enrichment_rules";
            int numberOfRows = context.fetchCount(context.selectFrom(tableName));
            Rule[] ruleArray = new Rule[numberOfRows];
            Result<Record6<Object, Object, Object, Object, Object, Object>> result = null;
            @NotNull SelectJoinStep<Record6<Object, Object, Object, Object, Object, Object>> result2;
            ArrayList<Rule> array = new ArrayList<>();
            String fieldNameFilterId = "enrichment_id";
            result2 = context.select(
                            field("enrichment_id"),
                            field("rule_id"),
                            field("field_name"),
                            field("field_name_enrichment"),
                            field("field_value"),
                            field("field_value_default")
                    )
                    .from(table(tableName));
            log.info("RESULT2 {}", result2);

            result = result2.fetch();

            log.info("RESULT {}", result.getValues(fieldNameFilterId).isEmpty());


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
                    e.printStackTrace();
                }
            });
            array.toArray(ruleArray);
            return ruleArray;
        }
        catch (SQLException e) {
            log.info("DB rules error!");
            throw new IllegalStateException("DB rules error");
        }
        catch (Exception e) {
            log.info("CAUGHT FETCH EX");
            return new Rule[0];
        }

    }
}
