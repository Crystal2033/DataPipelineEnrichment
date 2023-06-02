package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.FindIterable;
import com.typesafe.config.Config;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;
@Slf4j
@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    @NonNull
    Config config;
    @NonNull
    MongoClientImpl mongoClient;
    boolean isExit = false;

    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
//        if (Objects.equals(message.getValue(), "$exit")) {
//            isExit = true;
//        }

// To put all of the JSON in a Map<String, Object>
        try {
            Map<String, Object> map = mapper.readValue(message.getValue(), Map.class);
//            if (!isExit) {
                for (Rule rule : rules) {
                    //            ПРОВЕРКА ПРАВИЛ
                    log.info("RULES LENGTH {}", rules.length);
                    log.info("CHECKING FIELD {}", rule.getFieldName());
                    mongoClient.mongo(message, rule);
                }
//            } else {
//                message.setValue("$exit");
//            }
        } catch (JsonProcessingException e) {
            log.error("exception caught");
        } catch (Exception e) {
            log.info("caught null exception");
        }
        return message;
    }

//    private Message checkRule(Rule rule, Map<String, Object> map, Message message) {
////        JsonNode jsonNode = mapper.readTree(message.getValue());
//        Document document = new Document();
//        document.append(rule.getFieldNameEnrichment(), rule.getFieldValue());
//        FindIterable<Document> elements = mongoCollection.find(document).sort(new Document("_id", -1));
//        if (!elements.cursor().hasNext()) {
//            String d = rule.getFieldValueDefault();
//
//            JsonNode jsonNodeTemp = mapper.readTree(d);
//            ((ObjectNode) jsonNode).set(rule.getFieldName(), jsonNodeTemp);
////                    ((ObjectNode) jsonNode).set(rule.getFieldName(), rule.getFieldValueDefault());
//        } else {
//            String d = Objects.requireNonNull(elements.first()).toJson();
//            JsonNode jsonNodeTemp = mapper.readTree(d);
//            ((ObjectNode) jsonNode).set(rule.getFieldName(), jsonNodeTemp);
//        }
//        message.setValue(mapper.toString());
//        return message;
//    }
}

