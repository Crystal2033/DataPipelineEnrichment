package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record6;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.config.DbConfig;
import ru.mai.lessons.rpks.exception.ServerException;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;

@Slf4j
@Data
public class DbReaderImpl implements DbReader {

    private final DataSource dataSource;
    private final long updateIntervalSec;

    private Rule[] ruleArray;

    private Instant lastCheckTime;

    public DbReaderImpl(Config config) {
        this.dataSource = DbConfig.createConnectionPool(config);
        this.updateIntervalSec = config.getLong("application.updateIntervalSec");
    }

    @Override
    public Rule[] readRulesFromDB() {
        if (ruleArray == null ||
            Duration.between(lastCheckTime, Instant.now()).toMillis() > updateIntervalSec) {
            ruleArray = init();
            lastCheckTime = Instant.now();
        }
        return ruleArray;
    }


    // select * from enrichment_rules r where rule_id = (select max(rule_id) from enrichment_rules r1
    // where r.field_name = r1.field_name group by enrichment_id, field_name)
    private Rule[] init() {
        try (Connection connection = dataSource.getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = "enrichment_rules";
            String ruleIdField = "rule_id";
            var result = context.select(
                    field("enrichment_id"),
                    field("rule_id"),
                    field("field_name"),
                    field("field_name_enrichment"),
                    field("field_value"),
                    field("field_value_default")
                )
                .from(table(tableName).as("rule"))
                .where(field(ruleIdField)
                    .eq(select(max(field(ruleIdField)))
                        .from(table(tableName).as("rule1"))
                        .where(field("rule.field_name").eq(field("rule1.field_name")))
                        .groupBy(field("enrichment_id"), field("field_name")))
                )
                .fetch();  //

            return result.stream().map((Record6<Object, Object, Object, Object, Object, Object> r) -> {
                var rule = new Rule();
                rule.setEnricherId((Long) r.component1());
                rule.setRuleId((Long) r.component2());
                rule.setFieldName((String) r.component3());
                rule.setFieldNameEnrichment((String) r.component4());
                rule.setFieldValue((String) r.component5());
                rule.setFieldValueDefault((String) r.component6());
                return rule;
            }).toArray(Rule[]::new);
        } catch (SQLException e) {
            log.error("Не смогли получить соединение из базы");
            throw new ServerException("Не смогли получить соединение из базы", e);
        }
    }
}

