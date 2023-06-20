package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            SortedMap<String, String> ruleList = new TreeMap<>();
            JsonNode jsonNode = mapper.readTree(message.getValue());
            for (var rule : rules) {
                JsonNode temp = jsonNode.get(rule.getFieldName());
                if (temp == null)
                {
                    return message;
                }

                if (!temp.isValueNode())
                {
                    return message;
                }

                String value = jsonNode.get(rule.getFieldName()).asText();
                if (value == null)
                {
                    return message;
                }
                if (value.isEmpty())
                {
                    return message;
                }

                ruleList.put(rule.getFieldName(), value);
            }

            String joinedList = ruleList.entrySet()
                    .stream()
                    .map(el -> el.getKey() + el.getValue())
                    .collect(Collectors.joining("*", "{", "}"));


            return message;

        } catch (Exception e){
            log.info("Json error :{}", e.toString());
        }

        return message;
    }
}
