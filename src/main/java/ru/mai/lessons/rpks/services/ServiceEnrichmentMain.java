package ru.mai.lessons.rpks.services;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.configs.interfaces.ConfigReader;
import ru.mai.lessons.rpks.configs.ConfigurationReader;
import ru.mai.lessons.rpks.services.interfaces.Service;

@Slf4j
public class ServiceEnrichmentMain {
    public static void main(String[] args) {
        log.info("Start service Enrichment");
        ConfigReader configReader = new ConfigurationReader();
        Service service = new ServiceEnrichment(); // ваша реализация service
        service.start(configReader.loadConfig());
        log.info("Terminate service Enrichment");
//        try {
//            String txx = "{\"name\":\"alex\",\"age\":18,\"sex\":\"M\",\"enrichmentField\":{\"testFieldString\":\"testString\",\"testFieldNumeric\":1,\"testFieldArray\":[1],\"testFieldObject\":{\"testInnerFieldString\":\"testInnerString\"},\"condition_field_in_mongo\":\"condition_value\",\"_id\":{\"$oid\":\"647b4f12abaa6f7773d89b4e\"}},\"enrichmentOtherField\":null}";
//            String txt = "{\"new_field\":{\"_id\": {\"$oid\": \"647b8db9ecd591f9e1e91023\"}, \"testFieldString\": \"testString\", \"testFieldNumeric\": 1, \"testFieldArray\": [1], \"testFieldObject\": {\"testInnerFieldString\": \"testInnerString\"}, \"condition_field_in_mongo\": \"condition_value\", \"field_condition\": \"123\"},\"name\":\"Paul\",\"age\":20}";
//            JSONObject test = new JSONObject(txt);
//            System.out.printf("");
//        } catch (JSONException e) {
//            throw new RuntimeException(e);
//        }
    }
}