package ru.mai.lessons.rpks.processors.impl;

import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;

import java.util.List;
import java.util.Optional;

public class EnrichmentProcessor implements RuleProcessor {
    @Override
    public Optional<Message> processing(Message message, List<Rule> rules) {
        return Optional.empty();
    }
}
