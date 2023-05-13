/*
 * This file is generated by jOOQ.
 */
package ru.mai.lessons.rpks.jooq.model;


import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;

import ru.mai.lessons.rpks.jooq.model.tables.EnrichmentRules;
import ru.mai.lessons.rpks.jooq.model.tables.records.EnrichmentRulesRecord;


/**
 * A class modelling foreign key relationships and constraints of tables in
 * public.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<EnrichmentRulesRecord> ENRICHMENT_RULES_PKEY = Internal.createUniqueKey(EnrichmentRules.ENRICHMENT_RULES, DSL.name("enrichment_rules_pkey"), new TableField[] { EnrichmentRules.ENRICHMENT_RULES.ID }, true);
}