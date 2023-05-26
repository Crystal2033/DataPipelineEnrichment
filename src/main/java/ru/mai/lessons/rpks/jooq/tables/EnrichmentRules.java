/*
 * This file is generated by jOOQ.
 */
package ru.mai.lessons.rpks.jooq.tables;


import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function7;
import org.jooq.Identity;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row7;
import org.jooq.Schema;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import ru.mai.lessons.rpks.jooq.Keys;
import ru.mai.lessons.rpks.jooq.Public;
import ru.mai.lessons.rpks.jooq.tables.records.EnrichmentRulesRecord;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class EnrichmentRules extends TableImpl<EnrichmentRulesRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.enrichment_rules</code>
     */
    public static final EnrichmentRules ENRICHMENT_RULES = new EnrichmentRules();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<EnrichmentRulesRecord> getRecordType() {
        return EnrichmentRulesRecord.class;
    }

    /**
     * The column <code>public.enrichment_rules.id</code>.
     */
    public final TableField<EnrichmentRulesRecord, Integer> ID = createField(DSL.name("id"), SQLDataType.INTEGER.nullable(false).identity(true), this, "");

    /**
     * The column <code>public.enrichment_rules.enrichment_id</code>.
     */
    public final TableField<EnrichmentRulesRecord, Long> ENRICHMENT_ID = createField(DSL.name("enrichment_id"), SQLDataType.BIGINT.nullable(false), this, "");

    /**
     * The column <code>public.enrichment_rules.rule_id</code>.
     */
    public final TableField<EnrichmentRulesRecord, Long> RULE_ID = createField(DSL.name("rule_id"), SQLDataType.BIGINT.nullable(false), this, "");

    /**
     * The column <code>public.enrichment_rules.field_name</code>.
     */
    public final TableField<EnrichmentRulesRecord, String> FIELD_NAME = createField(DSL.name("field_name"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.enrichment_rules.field_name_enrichment</code>.
     */
    public final TableField<EnrichmentRulesRecord, String> FIELD_NAME_ENRICHMENT = createField(DSL.name("field_name_enrichment"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.enrichment_rules.field_value</code>.
     */
    public final TableField<EnrichmentRulesRecord, String> FIELD_VALUE = createField(DSL.name("field_value"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.enrichment_rules.field_value_default</code>.
     */
    public final TableField<EnrichmentRulesRecord, String> FIELD_VALUE_DEFAULT = createField(DSL.name("field_value_default"), SQLDataType.CLOB.nullable(false), this, "");

    private EnrichmentRules(Name alias, Table<EnrichmentRulesRecord> aliased) {
        this(alias, aliased, null);
    }

    private EnrichmentRules(Name alias, Table<EnrichmentRulesRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.enrichment_rules</code> table reference
     */
    public EnrichmentRules(String alias) {
        this(DSL.name(alias), ENRICHMENT_RULES);
    }

    /**
     * Create an aliased <code>public.enrichment_rules</code> table reference
     */
    public EnrichmentRules(Name alias) {
        this(alias, ENRICHMENT_RULES);
    }

    /**
     * Create a <code>public.enrichment_rules</code> table reference
     */
    public EnrichmentRules() {
        this(DSL.name("enrichment_rules"), null);
    }

    public <O extends Record> EnrichmentRules(Table<O> child, ForeignKey<O, EnrichmentRulesRecord> key) {
        super(child, key, ENRICHMENT_RULES);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public Identity<EnrichmentRulesRecord, Integer> getIdentity() {
        return (Identity<EnrichmentRulesRecord, Integer>) super.getIdentity();
    }

    @Override
    public UniqueKey<EnrichmentRulesRecord> getPrimaryKey() {
        return Keys.ENRICHMENT_RULES_PKEY;
    }

    @Override
    public EnrichmentRules as(String alias) {
        return new EnrichmentRules(DSL.name(alias), this);
    }

    @Override
    public EnrichmentRules as(Name alias) {
        return new EnrichmentRules(alias, this);
    }

    @Override
    public EnrichmentRules as(Table<?> alias) {
        return new EnrichmentRules(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public EnrichmentRules rename(String name) {
        return new EnrichmentRules(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public EnrichmentRules rename(Name name) {
        return new EnrichmentRules(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public EnrichmentRules rename(Table<?> name) {
        return new EnrichmentRules(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row7 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row7<Integer, Long, Long, String, String, String, String> fieldsRow() {
        return (Row7) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function7<? super Integer, ? super Long, ? super Long, ? super String, ? super String, ? super String, ? super String, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function7<? super Integer, ? super Long, ? super Long, ? super String, ? super String, ? super String, ? super String, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
