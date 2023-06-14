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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class RuleProcessorI implements RuleProcessor {
    MongoDBClient mongoDBClient;
    private final ObjectMapper mapper = new ObjectMapper();
    public RuleProcessorI(MongoDBClient redisClient){
        this.mongoDBClient = redisClient;
    }
    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules.length == 0){
            return message;
        }
        String value = message.getValue();
        var filteredRules = Arrays.stream(rules)
                .collect(Collectors.toMap(Rule::getFieldName, rule -> rule,
                        (oldValue, newValue) -> oldValue.getRuleId() > newValue.getRuleId() ? oldValue : newValue))
                .values();
        String fieldValue;
        try {
            JsonNode node = mapper.readTree(value);
            for (Rule rule : filteredRules) {
                String fieldName = rule.getFieldName();
                JsonNode val = node.path(fieldName);
                if (!val.isMissingNode()) {
                    Document doc = mongoDBClient.read(rule.getFieldNameEnrichment(), rule.getFieldValue());
                    fieldValue = rule.getFieldValueDefault();
                    ((ObjectNode) node).put(fieldName, fieldValue);
                    if (Optional.ofNullable(doc).isPresent()){
                        ((ObjectNode) node).set(fieldName, mapper.readTree(doc.toJson()));
                    }
                    message.setValue(node.toString());
                }

            }

        } catch (JsonProcessingException ex){
            log.error(ex.getMessage());
        }

        return message;
    }
}
