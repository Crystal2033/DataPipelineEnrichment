package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.impl.settings.DBSettings;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

@Setter
@Slf4j
@Builder
@Getter
public class ReaderFromDB implements DbReader {
    DBSettings dbSettings;
    @Override
    public Rule[] readRulesFromDB() {
        try (HikariDataSource hikariDataSource=new HikariDataSource(makeHikariConfig())){
            log.info("HIKARY_CREATE");
            DSLContext context = DSL.using(hikariDataSource.getConnection(), SQLDialect.POSTGRES);
            log.info("CONTEXT_MADE");
            Result<Record> information= context.select().from(dbSettings.getTableName()).fetch();
            Rule[] rules=new Rule[information.size()];
            log.info("RULLES_CREATE:"+information.size());
            int currentRuleIndex=0;
            for(Record ruleInformation:information){
                rules[currentRuleIndex]=Rule.builder().enricherId((Long)ruleInformation.get("enrichment_id"))
                        .ruleId((Long)ruleInformation.get("rule_id"))//rule_id
                        .fieldName((String) ruleInformation.get("field_name"))
                        .fieldValue((String) ruleInformation.get("field_value"))
                        .fieldValueDefault((String) ruleInformation.get("field_value_default"))
                        .fieldNameEnrichment((String) ruleInformation.get("field_name_enrichment")).build();
                log.info("FIND_RULE:"+rules[currentRuleIndex].toString());
                currentRuleIndex++;
            }
            log.info("MAKE_RULES_FROM_DB");
            return rules;
        } catch (SQLException e) {
            log.info("SQLException "+e.getMessage());
        }
        return new Rule[0];
    }
    HikariConfig makeHikariConfig(){
        HikariConfig hikariConfig=new HikariConfig();
        hikariConfig.setJdbcUrl(dbSettings.getJdbcUrl());
        hikariConfig.setUsername(dbSettings.getUser());
        hikariConfig.setPassword(dbSettings.getPassword());
        hikariConfig.setDriverClassName(dbSettings.getDriver());
        log.info("HIKARY_CREATE");
        return hikariConfig;
    }
}
