package ru.mai.lessons.rpks.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Data
@Builder
@Getter
@AllArgsConstructor
public class Message {
    private String value; // сообщение из Kafka в формате JSON
}
