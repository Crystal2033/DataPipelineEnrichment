package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    @NonNull
    Config config;
    @NonNull
    MongoImpl mongoDb;
    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);

        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());
            if (rules.length != 0) {
                for (Rule rule: rules) {
                    String doc = mongoDb.findDocument(rule.getFieldNameEnrichment(), rule.getFieldValue());
                    if (doc.equals("")) {
                        String field = rule.getFieldValueDefault();
                        ((ObjectNode) jsonNode).put(rule.getFieldName(), field);
                    } else {
                        JsonNode jsonNodeTemp = mapper.readTree(doc);
                        ((ObjectNode) jsonNode).set(rule.getFieldName(), jsonNodeTemp);
                    }
                    message.setValue(jsonNode.toString());
                }
            }
        } catch (JsonProcessingException e) {
            log.error("Error with json");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }
}
