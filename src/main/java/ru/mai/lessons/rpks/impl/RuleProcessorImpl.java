package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    private final MongoDBClient mongoDBClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            return message;
        }

        ObjectMapper objectMapper = new ObjectMapper();

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
                    log.debug("enrichment value {}", enrichmentValue);

                    JsonNode translatedToJsonNodeEnrichmentValue = translateStringToJsonNode(enrichmentValue);
                    objectNodeOfMessage.put(fieldNameToEnrich, translatedToJsonNodeEnrichmentValue);
                });

        String translatedObjectNodeToString = translateObjectNodeToStringJson(objectNodeOfMessage);
        log.debug("translated {}", translatedObjectNodeToString);
        return new Message(translatedObjectNodeToString);
    }

    private String getEnrichmentValueFromDocument(Rule rule, Optional<Document> documentOptional) {
        if (documentOptional.isEmpty()) {
            return rule.getFieldValueDefault();
        }

        Document enrichmentValueDocument = documentOptional.get();
        return enrichmentValueDocument.toJson();
    }

    private Optional<ObjectNode> translateMessageToObjectNodeOptional(Message message) {
        try {
            ObjectNode object = new ObjectMapper().readValue(message.getValue(), ObjectNode.class);
            return Optional.of(object);
        } catch (IOException e) {
            log.debug("exception : {}", e.getMessage());
            return Optional.empty();
        }
    }

    private JsonNode translateStringToJsonNode(String jsonString) {
        try {
            return objectMapper.readTree(jsonString);
        } catch (IOException e) {
            return objectMapper.createObjectNode();
        }
    }

    private String translateObjectNodeToStringJson(ObjectNode objectNode) {
        try {
            return objectMapper.writeValueAsString(objectNode);
        } catch (IOException e) {
            return "";
        }
    }

}
