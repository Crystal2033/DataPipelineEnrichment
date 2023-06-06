package ru.mai.lessons.rpks.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Enrichment;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import org.jooq.tools.json.*;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;

@Slf4j
@RequiredArgsConstructor
public class RuleProcess implements RuleProcessor {
    public Message processing(Message message, ArrayList<Enrichment> listEnrichments) throws ParseException {
        String jsonString = message.getValue();
        jsonString =  jsonString.replaceAll("[{}]", ""); // Удаляем {}

        final Map<String, String> map = new LinkedHashMap<>();
        final String[] elements = jsonString.split(",");
        for (final String element : elements) {
            final String[] keyValue = element.split(":");
            map.put(keyValue[0], keyValue[1]);
        }

        for (Enrichment enrichment : listEnrichments) {
            String fieldName = enrichment.getFieldName();              // поле сообщения, которое нужно обогатить
            String valueEnrichment = enrichment.getValueEnrichment(); // Value обогащения

            if (fieldName != null && valueEnrichment != null)
            {
                map.put("\"" + fieldName + "\"", valueEnrichment);
            }
        }

        StringBuilder jsonString2 = new StringBuilder("{");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            jsonString2.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }
        jsonString2.delete(jsonString2.length() - 1, jsonString2.length());
        jsonString2.append("}");

        message = Message.builder().value(jsonString2.toString()).build();

        return message;
    }

}
