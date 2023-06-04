package ru.mai.lessons.rpks.processors.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.mongo.impl.MongoEnrichmentClient;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class EnrichmentProcessor implements RuleProcessor {

    private final MongoEnrichmentClient mongoEnrichmentClient;

    @Override
    public Message processing(Message message, List<Rule> rules) {
        ObjectMapper objectMapper = new ObjectMapper();
        for (Rule rule : rules) {
            Optional<Document> document = mongoEnrichmentClient.getDocumentByRule(rule);
            document.ifPresentOrElse(doc -> {
                try {
                    String insertingJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(doc.toJson());
                    String goodJson = getGoodJsonAfterInsertion(insertingJson, message, rule);
                    message.setValue(goodJson);
                } catch (JSONException e) {
                    log.error("There is problem with appending new field in message. " + e.getMessage());
                } catch (JsonProcessingException e) {
                    log.error("There is problem with processing new field in message. " + e.getMessage());
                }
            }, () -> {
                try {
                    String insertingJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rule.getFieldValueDefault());
                    String goodJson = getGoodJsonAfterInsertion(insertingJson, message, rule);
                    message.setValue(goodJson);
                } catch (JSONException e) {
                    log.error("There is problem with appending new field in message. " + e.getMessage());
                } catch (JsonProcessingException e) {
                    log.error("There is problem with processing new field in message. " + e.getMessage());
                }
            });
        }
        try {
            JSONObject testJson = new JSONObject(message.getValue());
        } catch (JSONException e) {
            log.error("There is problem with parsing JSON. " + e.getMessage());
        }
        return message;
    }


    private String getGoodJsonAfterInsertion(String insertingJsonValue, Message message, Rule rule) throws JSONException {
        JSONObject jsonMessage = new JSONObject(message.getValue());
        insertingJsonValue = insertingJsonValue.substring(1, insertingJsonValue.length() - 1).replace("\\", "");
        jsonMessage.put(rule.getFieldName(), insertingJsonValue);
        StringBuilder goodJson = new StringBuilder(jsonMessage.toString());
        replaceAll(goodJson, "\\", "");
        replaceAll(goodJson, "\"" + rule.getFieldName() + "\":\"{", "\"" + rule.getFieldName() + "\":{");
        int insertingJsonStartIndex = goodJson.indexOf("\"" + rule.getFieldName() + "\"") + rule.getFieldName().length() + 2 + 1;
        //2 + 1 is for '\"'  and ':' and plus 1 for startingIndex.
        // Here I want to get the end of insertingJsonValue in fullJson (with inserted new json) to
        //carefully delete last '\"' after closing } if it exists

        if(goodJson.charAt(insertingJsonStartIndex + insertingJsonValue.length()) == '\"'){
            goodJson.replace(insertingJsonStartIndex + insertingJsonValue.length(), insertingJsonStartIndex + insertingJsonValue.length() + 1, "");
        }
        return goodJson.toString();
    }

    private static void replaceAll(StringBuilder builder, String from, String to) {
        int index = builder.indexOf(from);
        while (index != -1) {
            builder.replace(index, index + from.length(), to);
            index += to.length(); // Move to the end of the replacement
            index = builder.indexOf(from, index);
        }
    }
//    private JsonNode toJsonNode(String json) {
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode jsonNode = objectMapper.createObjectNode();
//        try {
//            jsonNode = objectMapper.readTree(json);
//        } catch (IOException e) {
//            log.error("Error transformation json string to json node {}", json);
//        }
//        return jsonNode;
//    }
}
