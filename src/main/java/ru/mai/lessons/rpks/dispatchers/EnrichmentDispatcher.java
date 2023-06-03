package ru.mai.lessons.rpks.dispatchers;

import com.typesafe.config.Config;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import ru.mai.lessons.rpks.exceptions.ThreadWorkerNotFoundException;
import ru.mai.lessons.rpks.kafka.impl.KafkaWriterImpl;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;
import ru.mai.lessons.rpks.repository.impl.RulesUpdaterThread;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class EnrichmentDispatcher {
    private static final String KAFKA_NAME = "kafka";
    private static final String TOPIC_NAME_PATH = "topic.name";
    private final Config config;
    private final RulesUpdaterThread updaterRulesThread; //to get actual rules, which are in db thread reader
    private KafkaWriterImpl kafkaWriter;

    private List<Rule> rulesList;

    private final RuleProcessor ruleProcessor;

    public void updateRules() throws ThreadWorkerNotFoundException {
        rulesList = Optional.ofNullable(updaterRulesThread).
                orElseThrow(() -> new ThreadWorkerNotFoundException("Database updater not found")).getRules();
    }


    public void actionWithMessage(String msgStr) throws ThreadWorkerNotFoundException {
        kafkaWriter = Optional.ofNullable(kafkaWriter).orElseGet(this::createKafkaWriterForSendingMessage);
        updateRules();
        if (rulesList.isEmpty()) {
            kafkaWriter.processing(getMessage(msgStr));
        } else {
            log.info("Before processor: " + msgStr);
            Message message = ruleProcessor.processing(getMessage(msgStr), rulesList);
            log.info("After processor: " + message.getValue());
            kafkaWriter.processing(message);
        }
    }

    private KafkaWriterImpl createKafkaWriterForSendingMessage() {
        Config producerKafkaConfig = config.getConfig(KAFKA_NAME).getConfig("producer");
        return KafkaWriterImpl.builder()
                .topic(producerKafkaConfig.getConfig("final").getString(TOPIC_NAME_PATH))
                .bootstrapServers(producerKafkaConfig.getString("bootstrap.servers"))
                .build();
    }

    private Message getMessage(String value) {
        return Message.builder()
                .value(value)
                .build();
    }
}
