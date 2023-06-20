package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    private final MongoDBClientEnricher mongoDBClientEnricher;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message processing(Message message, Rule[] rules) {
        JsonNode jsonMessage = null;
        if (!Objects.equals(message.getValue(), "")) {
            try {
                jsonMessage = objectMapper.readTree(message.getValue());
                for (Rule rule : rules) {
                    Document enrichDocument =
                            mongoDBClientEnricher.getEnrichmentRules(rule.getFieldNameEnrichment(), rule.getFieldValue());
                    if (enrichDocument != null) {
                        ((ObjectNode)jsonMessage).set(rule.getFieldName(), objectMapper.readTree(enrichDocument.toJson()));
                    } else {
                        ((ObjectNode)jsonMessage).put(rule.getFieldName(), rule.getFieldValueDefault());
                    }
                }
            } catch (Exception ex) {
                log.error(ex.getMessage());
            }
        }
        if (jsonMessage != null) {
            message.setValue(jsonMessage.toString());
        }
        return message;
    }
}
