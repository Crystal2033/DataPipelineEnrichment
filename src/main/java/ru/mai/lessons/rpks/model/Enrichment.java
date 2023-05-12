package ru.mai.lessons.rpks.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Enrichment {
    private String fieldName;        // поле сообщения, которое нужно обогатить
    private String valueEnrichment; // Value обогащения
}
