package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.exception.ServerException;
import ru.mai.lessons.rpks.model.Rule;

import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    private final ObjectMapper objectMapper;
    private final MongoDBClientEnricher mongoDBClientEnricher;

    public RuleProcessorImpl(Config config) {
        this.objectMapper = new ObjectMapper();
        this.mongoDBClientEnricher = new MongoDBClientEnricherImpl(config);
    }

    @Override
    public String apply(ConsumerRecord<String, String> consumerRecord, Rule[] rules) {
        log.debug("Check rules for record {}", consumerRecord.value());

        if (rules == null || rules.length == 0) {
            return consumerRecord.value();
        }

        ObjectNode objectNode;
        try {
            objectNode = objectMapper.readValue(consumerRecord.value(), new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            log.error("Не смогли создать json", e);
            return consumerRecord.value();
        }


        Stream.of(rules).forEach(r ->
                objectNode.put(r.getFieldName(), mongoDBClientEnricher.read(r.getFieldNameEnrichment(), r.getFieldValue())));
        try {
            return objectMapper.writeValueAsString(objectNode);
        } catch (JsonProcessingException e) {
            throw new ServerException("Unknown error while serialization");
        }
    }

}
