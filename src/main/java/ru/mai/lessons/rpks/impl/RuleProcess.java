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
    //private final MongoDB mongoDB;
    public Message processing(Message message, Enrichment enrichment) throws ParseException {
        log.info("-------- MESSAGE {}", message.getValue());

        String jsonString = message.getValue();
        jsonString =  jsonString.replaceAll("[{}]", ""); // Удаляем {}

        final Map<String, String> map = new LinkedHashMap<>();
        final String[] elements = jsonString.split(",");
        for (final String element : elements) {
            final String[] keyValue = element.split(":");
            map.put(keyValue[0], keyValue[1]);
        }

        log.info("+++++++++++ map {}", map);

        //JSONObject jsonObject = new JSONObject(map);
//        String jsonString3 = jsonObject.toString();
//        log.info("+++++++++++ jsonString3 {}", jsonString3);

        //jsonString =  jsonString.replace(":-", ":null");
        //jsonString =  jsonString.replace(":,", ":null,");
//        jsonString =  jsonString.replace(":", ":\"");
//        jsonString =  jsonString.replace(",", "\",");
//        jsonString =  jsonString.replace("\"null", "null");
//        JSONParser jsonParser = new JSONParser();
//        JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);

        String fieldName = enrichment.getFieldName();              // поле сообщения, которое нужно обогатить
        String valueEnrichment = enrichment.getValueEnrichment(); // Value обогащения

        if (fieldName != null && valueEnrichment != null)
        {
            map.put("\"" + fieldName + "\"", valueEnrichment);
            log.info("+++++++++++ map2 {}", map);

            StringBuilder jsonString2 = new StringBuilder("{");
            for (Map.Entry<String, String> entry : map.entrySet()) {
                jsonString2.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            jsonString2.delete(jsonString2.length() - 1, jsonString2.length());
            jsonString2.append("}");
            log.info("=========== jsonString2 {}", jsonString2);

//            jsonObject.remove(fieldName);
//            jsonObject.put(fieldName, valueEnrichment);
//            log.info("=========== jsonObject {}", jsonObject);
//            log.info("=========== jsonString {}", jsonObject.toString());

            message = Message.builder().value(jsonString2.toString()).build();
            log.info("=========== message {}", message.getValue());
            return message;
        }







        return message;
    }

//    private final ObjectMapper objectMapper = new ObjectMapper();
//
//    private String toJson(Object object) {
//        String json = "{}";
//        try {
//            json = objectMapper.writeValueAsString(object);
//        } catch (JsonProcessingException e) {
//            log.error("Error convert object to json", e);
//        }
//        return json;
//    }
}
