package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.model.Rule;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RuleProcessor {
    String apply(ConsumerRecord<String, String> consumerRecord, Rule[] rules);
}
