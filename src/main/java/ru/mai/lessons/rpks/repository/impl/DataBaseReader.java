package ru.mai.lessons.rpks.repository.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.PlainSQL;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.repository.interfaces.DbReader;

import java.sql.Connection;
import java.sql.SQLException;

import static org.jooq.impl.DSL.field;

@PlainSQL
@Slf4j
@Builder
public class DataBaseReader implements DbReader, AutoCloseable {
    private final String url;
    private final String userName;
    private final String password;
    private final String driver;

    private final Config additionalDBConfig;

    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;

    private DSLContext dslContext;

    private Connection dataSourceConnection;

    public boolean connectToDataBase() throws SQLException {
        initHikariConfig();
        if (dataSource == null) {
            initDataSourceAndDSLContext();
        } else {
            dataSourceConnection.close();
        }
        dataSourceConnection = dataSource.getConnection();
        return isConnectedToDataBase();
    }

    public boolean isConnectedToDataBase() throws SQLException {
        return dataSourceConnection.isValid(additionalDBConfig.getInt("connect_valid_time"));
    }

    @Override
    public Rule[] readRulesFromDB() {
        try {
            if (isConnectedToDataBase()) {
                return dslContext.select()
                        .from(additionalDBConfig.getString("table_name"))
                        .where(field(additionalDBConfig.getString("enrichment_id_column_name"))
                                .eq(additionalDBConfig.getInt("enrichment_id")))
                        .fetch()
                        .stream()
                        .map(note -> Rule.builder()
                                .ruleId((Long) note.get("rule_id"))
                                .enricherId((Long) note.get("enrichment_id"))
                                .fieldName(note.get("field_name").toString())
                                .fieldNameEnrichment(note.get("field_name_enrichment").toString())
                                .fieldValue(note.get("field_value").toString())
                                .fieldValueDefault(note.get("field_value_default").toString())
                                .build())
                        .toList().toArray(new Rule[0]);
            }
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        }
        return new Rule[0];
    }

    @Override
    public void close() {
        dataSource.close();
    }

    private void initHikariConfig() {
        config.setJdbcUrl(url);
        config.setUsername(userName);
        config.setPassword(password);
        config.setDriverClassName(driver);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    }

    private void initDataSourceAndDSLContext() {
        dataSource = new HikariDataSource(config);
        dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);
    }


}
