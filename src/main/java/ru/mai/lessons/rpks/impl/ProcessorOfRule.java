package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.tools.json.JSONObject;
import org.jooq.tools.json.JSONParser;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
@Builder
public class ProcessorOfRule implements RuleProcessor {

    @Override
    public Message processing(Message message, Rule[] rules)   {
        return message;
    }

}
