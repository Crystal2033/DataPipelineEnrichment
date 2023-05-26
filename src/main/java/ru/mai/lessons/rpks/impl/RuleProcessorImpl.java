package ru.mai.lessons.rpks.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private final MongoDBClientEnricher mongoClient;

    @Override
    public Message processing(Message message, Rule[] rules) {
        if ((rules == null) || (rules.length == 0))
            return message;

        JsonObject object = convertStringToJsonObject(message.getValue());
        if (object.isEmpty())
            return message;

        Map<String, Rule> namesWithRules = new HashMap<>();
        Arrays.stream(rules).forEach(rule -> namesWithRules.merge(
                        rule.getFieldName(),
                        rule,
                        (value, newValue) -> value.getRuleId() > newValue.getRuleId() ? value : newValue
                )
        );
        namesWithRules.entrySet()
                .stream()
                .filter(entry -> object.has(entry.getKey()))
                .forEach(entry -> {
                    Rule rule = entry.getValue();
                    Document doc = mongoClient.getDoc(rule.getFieldNameEnrichment(),rule.getFieldValue());
                    String newValue = getNewValueFromDoc(rule, doc);
                    log.info("New value: {}", newValue);

                    if (Optional.ofNullable(doc).isEmpty())
                        object.addProperty(entry.getKey(), newValue);
                    else
                        object.add(entry.getKey(), convertStringToJsonObject(newValue));
                });
        return new Message(convertJsonObjectToString(object));
    }

    private JsonObject convertStringToJsonObject(String jsonString) {
        try {
            return JsonParser.parseString(jsonString).getAsJsonObject();
        } catch (JsonParseException | IllegalStateException e) {
            return new JsonObject();
        }
    }

    private String convertJsonObjectToString(JsonObject object) {
        try {
            return object.toString();
        } catch (IllegalStateException e) {
            return "";
        }
    }

    private String getNewValueFromDoc(Rule rule, Document doc) {
        if (Optional.ofNullable(doc).isEmpty())
            return rule.getFieldValueDefault();
        return doc.toJson();
    }
}
