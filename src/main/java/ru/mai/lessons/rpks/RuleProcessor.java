package ru.mai.lessons.rpks;

import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.model.Enrichment;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

public interface RuleProcessor {
    public Message processing(Message message, Enrichment enrichment) throws ParseException; // применяет правила обогащения к сообщениям и вставляет документы из MongoDB в указанные поля сообщения, если сообщение удовлетворяет условиям всех правил.
}
