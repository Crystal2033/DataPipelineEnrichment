package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EnrichmentRuleProcessor implements RuleProcessor {

    MongoDBClient mongoDBClient;
    ObjectMapper mapper = new ObjectMapper();
    @Override
    public Message processing(Message message, Rule[] rules) {
        try {
            Map<String, JsonNode> map;
            Map<String, String> nonJsonMap = new HashMap<>();
            map = mapper.readValue(message.getValue(), new TypeReference<Map<String, JsonNode>>() {});

            for (Rule rule : rules) {
                Document enrichRule = mongoDBClient.getEnrichmentRules(rule.getFieldNameEnrichment(), rule.getFieldValue());

                if (enrichRule != null) {
                    map.put(rule.getFieldName(), mapper.readTree(enrichRule.toJson()));
                }
                else {
                    nonJsonMap.put(rule.getFieldName(),rule.getFieldValueDefault());
                }
                log.info("message " + message.getValue() + " get new value in " + rule.getFieldName() + " =" + map.get(rule.getFieldName()));
            }

            JsonNode newMessage = mapper.readTree(mapper.writeValueAsString(map));
            for (var it : nonJsonMap.entrySet())
            {
                ((ObjectNode)newMessage).put(it.getKey(), it.getValue());
            }
            message.setValue(newMessage.toString());

            log.info("message has been enriched");
            return message;
        } catch (
        JsonProcessingException e) {
            log.info("Message {} have uncorrected data", message.getValue());
            log.error("JSON Error" + e.getMessage());
            return null;
    }
    }

    public void createMongoClient(Config config) {
        mongoDBClient = new MongoDBClient(config);
    }
}
