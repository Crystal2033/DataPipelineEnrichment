package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;


@Slf4j
@Builder
public class KafkaWriterImpl implements KafkaWriter {
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesGetter;
    private final String topic;
    private final String bootstrapServers;

    private final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
            Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
    );

    @Override
    public void processing(Message message) {
        Message checkedMessage = ruleProcessor.processing(message, rulesGetter.get());
        kafkaProducer.send(new ProducerRecord<>(topic, checkedMessage.getValue()));
    }
}
