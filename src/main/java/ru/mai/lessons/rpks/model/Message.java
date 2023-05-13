package ru.mai.lessons.rpks.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Message {
    private final String value; // сообщение из Kafka в формате JSON
}
