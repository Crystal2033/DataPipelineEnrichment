package ru.mai.lessons.rpks.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MessageRuleProcessor implements RuleProcessor {

    private final MyMongoDBClientEnricher mongoClient;

    public MessageRuleProcessor(MyMongoDBClientEnricher mongoClient) {
        this.mongoClient = mongoClient;
        log.info("MessageRuleProcessor created");
    }

    private final ObjectMapper parser = new ObjectMapper();
    @Override
    public Message processing(Message message, Rule[] rules) {
        String value = message.getValue();
        log.debug("Message = {}", value);
        if (rules.length == 0) {
            return message;
        }

        try {
            JsonNode jsonNode = parser.readTree(value);

            Arrays.stream(rules)
                    .collect(Collectors.toMap(Rule::getFieldName, rule -> rule,
                            (oldR, newR) -> oldR.getRuleId() > newR.getRuleId() ? oldR : newR))
                    .forEach((fieldName, rule) -> {
                if (!jsonNode.path(rule.getFieldName()).isMissingNode()) {
                    Document mongoDoc = mongoClient.read(rule.getFieldNameEnrichment(), rule.getFieldValue());
                    String fieldValue = rule.getFieldValueDefault();
                    log.debug("Default value = {}", rule.getFieldValueDefault());
                    ((ObjectNode) jsonNode).put(rule.getFieldName(), fieldValue);

                    if (Optional.ofNullable(mongoDoc).isPresent()) {
                        fieldValue = mongoDoc.toJson();
                        try {
                            ((ObjectNode) jsonNode).set(rule.getFieldName(), parser.readTree(fieldValue));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                        log.debug("JSON NODE = {}", jsonNode);
                        message.setValue(jsonNode.toString());

                }
            });
        }
        catch (JsonProcessingException  e) {
            return message;
        }
        return message;
    }
}
