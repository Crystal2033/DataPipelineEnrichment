package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.MongoDBClient;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@RequiredArgsConstructor
public class RuleProcessorImpl implements RuleProcessor {
    private final MongoDBClient mongoDBClient;

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            return message;
        }


        return null;
    }
}
