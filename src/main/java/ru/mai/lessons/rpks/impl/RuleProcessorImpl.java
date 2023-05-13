package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    private final MongoDBClient mongoDBClient;

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            return message;
        }

        Optional<ObjectNode> objectNodeOfMessageOptional = translateMessageToObjectNodeOptional(message);
        if (objectNodeOfMessageOptional.isEmpty()) {
            return message;
        }
        ObjectNode objectNodeOfMessage = objectNodeOfMessageOptional.get();

        Map<String, Rule> fieldNameToEnrichAndItsRule = new HashMap<>();
        Arrays.stream(rules).forEach(rule -> fieldNameToEnrichAndItsRule.merge(
                        rule.getFieldName(),
                        rule,
                        (oldValue, newValue) -> oldValue.getRuleId() > newValue.getRuleId() ? oldValue : newValue
                )
        );

        fieldNameToEnrichAndItsRule.entrySet()
                .stream()
                .filter(entry -> objectNodeOfMessage.has(entry.getKey()))
                .forEach(entry -> {
                    String fieldNameToEnrich = entry.getKey();
                    Rule rule = entry.getValue();

                    Optional<Document> documentOptional = mongoDBClient.getDocument(
                            rule.getFieldNameEnrichment(),
                            rule.getFieldValue()
                    );

                    String enrichmentValue = getEnrichmentValueFromDocument(rule, documentOptional);
                    objectNodeOfMessage.put(fieldNameToEnrich, enrichmentValue);
                });

        return null;
    }

    private String getEnrichmentValueFromDocument(Rule rule, Optional<Document> documentOptional) {
        if (documentOptional.isEmpty()) {
            return rule.getFieldValueDefault();
        }

        Document document = documentOptional.get();
        return document.getString(rule.getFieldName());
    }

    private Optional<ObjectNode> translateMessageToObjectNodeOptional(Message message) {
        try {
            ObjectNode object = new ObjectMapper().readValue(message.getValue(), ObjectNode.class);
            return Optional.of(object);
        } catch (IOException e) {
            return Optional.empty();
        }
    }


}
