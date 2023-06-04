package ru.mai.lessons.rpks.processors.interfaces;

import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.List;

public interface RuleProcessor {
    Message processing(Message message, List<Rule> rules); // применяет правила обогащения к сообщениям и вставляет документы из MongoDB в указанные поля сообщения, если сообщение удовлетворяет условиям всех правил.
}
