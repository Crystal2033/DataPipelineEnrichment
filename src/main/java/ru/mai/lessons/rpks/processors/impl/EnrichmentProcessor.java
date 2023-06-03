package ru.mai.lessons.rpks.processors.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.mongo.impl.MongoEnrichmentClient;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class EnrichmentProcessor implements RuleProcessor {

    private final MongoEnrichmentClient mongoEnrichmentClient;

    @Override
    public Message processing(Message message, List<Rule> rules) throws JSONException {
        JSONObject jsonMessage = new JSONObject(message.getValue());
        ObjectMapper objectMapper = new ObjectMapper();
        for (Rule rule : rules) {
            Optional<Document> document = mongoEnrichmentClient.getDocumentByRule(rule);
            document.ifPresentOrElse(doc -> {
                try {
//                    JsonNode jsonNode = toJsonNode(doc.toJson());
//                    log.info(jsonNode.toString());

                    JsonNode tree = objectMapper.readTree(doc.toJson());
                    if (jsonMessage.has(rule.getFieldName())) {
                        jsonMessage.put(rule.getFieldName(), doc.toJson());
                        tree = objectMapper.readTree(jsonMessage.toString());
                        log.info(tree.toString());
                    } else {
                        jsonMessage.append(rule.getFieldName(), doc.toJson());
                    }
                } catch (JSONException e) {
                    log.error("There is problem with appending new field in message. " + e.getMessage());
                } catch (JsonMappingException e) {
                    log.error("Problem with readTree mapping exception " + e.getMessage());
                } catch (JsonProcessingException e) {
                    log.error("Problem with readTree processing exception " + e.getMessage());
                }
            }, () -> {
                try {
                    if (jsonMessage.has(rule.getFieldName())) {
                        jsonMessage.put(rule.getFieldName(), rule.getFieldValueDefault());
                    } else {
                        jsonMessage.append(rule.getFieldName(), rule.getFieldValueDefault());
                    }
                } catch (JSONException e) {
                    log.error("There is problem with appending new field in message. " + e.getMessage());
                }
            });
        }
        message.setValue(toJsonNode(jsonMessage.toString()).toString());
        return message;
    }

    private JsonNode toJsonNode(String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.createObjectNode();
        try {
            jsonNode = objectMapper.readTree(json);
        } catch (IOException e) {
            log.error("Error transformation json string to json node {}", json);
        }
        return jsonNode;
    }
}
