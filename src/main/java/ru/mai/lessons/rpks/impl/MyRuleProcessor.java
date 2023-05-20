package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.AllArgsConstructor;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;


@AllArgsConstructor
public class MyRuleProcessor implements RuleProcessor {
    Config config;
    MyMongo myMongo;

    public MyRuleProcessor(Config config) {
        this.config = config;
        myMongo = new MyMongo(config);
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        for (Rule rule : rules) {
            myMongo.checkAndReplace(message, rule);
        }

        return message;
    }
}
