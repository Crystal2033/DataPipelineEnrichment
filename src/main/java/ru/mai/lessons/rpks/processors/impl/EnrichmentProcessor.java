package ru.mai.lessons.rpks.processors.impl;

import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.json.JSONObject;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.mongo.impl.MongoEnrichmentClient;
import ru.mai.lessons.rpks.processors.interfaces.RuleProcessor;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class EnrichmentProcessor implements RuleProcessor {

private final MongoEnrichmentClient mongoEnrichmentClient;
    @Override
    public Optional<Message> processing(Message message, List<Rule> rules) {
        //JSONObject jsonMessage = new JSONObject(message.getValue());

        for(Rule rule : rules){
            Optional<Document> document = mongoEnrichmentClient.getDocumentByRule(rule);
            document.ifPresent((doc) -> {
                //message.getValue()
            });
        }
        //TODO: setting enrichment into message. We have unique fieldName rules. Need get document and set new info
        return Optional.empty();
    }
}
