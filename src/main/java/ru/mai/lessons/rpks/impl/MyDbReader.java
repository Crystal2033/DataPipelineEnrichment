package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

@Slf4j
public class MyDbReader implements DbReader {
    private final HikariDataSource ds;
    private String nameDb;

    public MyDbReader(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(config.getString("user"));
        hikariConfig.setPassword(config.getString("password"));
        hikariConfig.setJdbcUrl(config.getString("jdbcUrl"));
        nameDb = config.getString("nameDb");
        ds = new HikariDataSource(hikariConfig);

    }

    @Override
    public Rule[] readRulesFromDB() {
        ArrayList<Rule> result = new ArrayList<>();
        try {
            Connection connection = ds.getConnection();
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            Result<Record> rules = context.select().from(nameDb).orderBy(DSL.param("ruleId")).fetch();
            rules.forEach(e -> {
                Long enricherId = (Long) e.getValue("enrichment_id");
                Long ruleId = (Long) e.getValue("rule_id");
                String fieldName = (String) e.getValue("field_name");
                String fieldNameEnrichment = (String) e.getValue("field_name_enrichment");
                String fieldValue = (String) e.getValue("field_value");
                String fieldValueDefault = (String) e.getValue("field_value_default");
                Rule rule = new Rule(enricherId, ruleId, fieldName, fieldNameEnrichment, fieldValue, fieldValueDefault);
                result.add(rule);
            });

            connection.close();
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

        return result.toArray(new Rule[0]);
    }
}
