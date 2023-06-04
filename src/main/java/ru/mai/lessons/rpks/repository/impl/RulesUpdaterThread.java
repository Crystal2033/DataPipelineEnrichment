package ru.mai.lessons.rpks.repository.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Slf4j
@Getter
public class RulesUpdaterThread implements Runnable {

    private List<Rule> rules;

    private final DataBaseReader dataBaseReader;

    private final Config configForSleep;

    private List<Rule> makeUniqueListWithLatestInfo(List<Rule> rulesFromDB) {
        Map<String, Rule> uniqueFieldNames = new HashMap<>();
        for (Rule rule : rulesFromDB) {
            if (uniqueFieldNames.containsKey(rule.getFieldName())) {
                if (uniqueFieldNames.get(rule.getFieldName()).getRuleId() < rule.getRuleId()) {
                    uniqueFieldNames.put(rule.getFieldName(), rule);
                }
            } else {
                uniqueFieldNames.put(rule.getFieldName(), rule);
            }
        }
        return new ArrayList<>(uniqueFieldNames.values());
    }

    @Override
    public void run() {
        rules = makeUniqueListWithLatestInfo(List.of(dataBaseReader.readRulesFromDB()));
        for (Rule rule : rules) {
            log.info(rule.getFieldName() + " " + rule.getFieldValue());
        }
    }
}
