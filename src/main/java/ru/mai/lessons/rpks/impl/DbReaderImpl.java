package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;

@Slf4j
public class DbReaderImpl implements DbReader {
    private static final HikariConfig hikariConfig = new HikariConfig();
    private final HikariDataSource ds;
    public DbReaderImpl(Config config) {
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));
        ds = new HikariDataSource(hikariConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        try {
            var con = ds.getConnection();
            DSLContext context = DSL.using(con, SQLDialect.POSTGRES);
            return context.select().from("public.enrichment_rules").
                    fetch().
                    stream().
                    map(re ->Rule.builder().
                            enricherId((Long) re.getValue("enrichment_id")).
                            ruleId((Long) re.getValue("rule_id")).
                            fieldName((String) re.getValue("field_name")).
                            fieldNameEnrichment((String) re.getValue("field_name_enrichment")).
                            fieldValue((String) re.getValue("field_value")).
                            fieldValueDefault((String) re.getValue("field_value_default")).build()
                    ).toList().toArray(Rule[]::new);
            
        } catch (SQLException e) {
            log.error("can't read from db error: {}", e.toString());
        }
        return new Rule[0];
    }
}
