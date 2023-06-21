package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.AbstractMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    ObjectMapper mapper = new ObjectMapper();
    MongoDBClientEnricher mongo;

    RuleProcessorImpl(MongoDBClientEnricher mongoDBClientEnricher){
        mongo = mongoDBClientEnricher;
    }

    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            SortedMap<String, Map.Entry<Long, String>> ruleList = new TreeMap<>();
            JsonNode jsonNode = mapper.readTree(message.getValue());
            String fieldValue;

            for (var rule : rules) {
                fieldValue = mongo.getFile(rule);
                var node = new AbstractMap.SimpleEntry<>(rule.getRuleId(), fieldValue);
                if (ruleList.containsKey(rule.getFieldName())){
                    if (rule.getRuleId() > ruleList.get(rule.getFieldName()).getKey()){
                        ruleList.put(rule.getFieldName(), node);
                    }
                }
                else {
                    ruleList.put(rule.getFieldName(), node);
                }
            }


            for (var el: ruleList.entrySet()){
                ((ObjectNode)jsonNode).set(el.getKey(), mapper.readTree(el.getValue().getValue()));
            }

            message.setValue(jsonNode.toString());

            return message;

        } catch (Exception e){
            log.info("Json error :{}", e.toString());
        }

        return message;
    }
}
