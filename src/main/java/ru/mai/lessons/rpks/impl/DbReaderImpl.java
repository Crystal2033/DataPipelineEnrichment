package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.jooq.impl.DSL.field;

@Slf4j
@RequiredArgsConstructor
@Setter
public class DbReaderImpl implements DbReader {

    private DataSource dataSource;
    @NonNull
    Config config;

    private static final String ENRICHMENT_ID = "enrichment_id";
    private static final String RULE_ID = "rule_id";
    private static final String FIELD_NAME = "field_name";
    private static final String FIELD_NAME_ENRICHMENT = "field_name_enrichment";
    private static final String FIELD_VALUE = "field_value";
    private static final String FIELD_VALUE_DEFAULT = "field_value_default";

    private Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private DataSource getDataSource() {
        if (dataSource == null) {
            createDataSource();
        }
        log.info("Get datasource successfully");
        return dataSource;
    }

    void createDataSource() {
        try {
            String driver = config.getString("db.driver");
            Class.forName(driver);
            HikariConfig hikariConfigConfig = createHikariConfig();
            this.dataSource = new HikariDataSource(hikariConfigConfig);
            log.info("Created a new datasource");
        } catch (ClassNotFoundException e) {
            log.error("Class not found exception (create data source)");
        }
    }

    HikariConfig createHikariConfig() {
        String driver = config.getString("db.driver");
        HikariConfig hikariConfig = new HikariConfig();
        try {
            Class.forName(driver);
            hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
            hikariConfig.setUsername(config.getString("db.user"));
            hikariConfig.setPassword(config.getString("db.password"));
            hikariConfig.setDriverClassName(driver);
            log.info("Hikari config was done");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return hikariConfig;
    }

    @Override
    public Rule[] readRulesFromDB() {

        try (Connection connection = getConnection()){
            DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = config.getString("db.table");

            ArrayList<Rule> rulesFromDb = new ArrayList<>();
            ArrayList<String> namesEnrichment = new ArrayList<>();

            var selectFromDb = dsl.select().from(tableName).orderBy(field(RULE_ID)).fetch();

            selectFromDb.forEach(row -> {
                Long fieldEnrichmentId = (Long) row.getValue(field(ENRICHMENT_ID));
                Long fieldRuleId = (Long) row.getValue(field(RULE_ID));
                String fieldFieldName = row.getValue(field(FIELD_NAME)).toString();
                String fieldFieldNameEnrichment = row.getValue(field(FIELD_NAME_ENRICHMENT)).toString();
                String fieldFieldValue = row.getValue(field(FIELD_VALUE)).toString();
                String fieldDefault = row.getValue(field(FIELD_VALUE_DEFAULT)).toString();
                Rule rule = new Rule(fieldEnrichmentId, fieldRuleId, fieldFieldName, fieldFieldNameEnrichment, fieldFieldValue, fieldDefault);
                int indexRule;
                String checkActualRule = fieldFieldNameEnrichment + ";" + fieldFieldName;
                if ((indexRule = namesEnrichment.indexOf(checkActualRule)) != -1) {
                    rulesFromDb.set(indexRule, rule);
                    namesEnrichment.set(indexRule, checkActualRule);
                } else {
                    rulesFromDb.add(rule);
                    namesEnrichment.add(checkActualRule);
                }
            });
            Rule[] rules = new Rule[rulesFromDb.size()];
            rulesFromDb.toArray(rules);
            return rules;
        } catch (SQLException e) {
            log.error("Connection was failed!");
            throw new IllegalStateException("DB is not ready");
        }
        catch (Exception e) {
            e.printStackTrace();
            return new Rule[0];
        }
    }
}
