package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
    MongoClientImpl mongoClient;

    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);


// To put all of the JSON in a Map<String, Object>
        try {
                for (Rule rule : rules) {
                    //            ПРОВЕРКА ПРАВИЛ
                    log.info("RULES LENGTH {}", rules.length);
                    log.info("CHECKING FIELD {}", rule.getFieldName());
                    mongoClient.mongo(message, rule);
                }

        } catch (Exception e) {
            log.error("caught null exception");
        }
        return message;
    }


}

