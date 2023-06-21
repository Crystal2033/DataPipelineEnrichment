package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.exception.ServerException;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Optional;
import java.util.stream.Stream;

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


        try {
            Stream.of(rules).forEach(r -> {
                objectNode.set(r.getFieldName(),
                    Optional.ofNullable(mongoDBClientEnricher.read(r.getFieldNameEnrichment(), r.getFieldValue()))
                        .map(s -> {
                            try {
                                return (JsonNode) objectMapper.readValue(s, ObjectNode.class);
                            } catch (JsonProcessingException e) {
                                throw new ServerException("Unknown error while serialization");
                            }
                        }).orElse(new TextNode(r.getFieldValueDefault())));
            });
            return objectMapper.writeValueAsString(objectNode);
        } catch (JsonProcessingException e) {
            throw new ServerException("Unknown error while serialization");
        }
    }

}
