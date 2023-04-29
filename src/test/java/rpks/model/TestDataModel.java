package rpks.model;

import com.fasterxml.jackson.annotation.JsonRawValue;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TestDataModel {
    @JsonRawValue
    private String name;
    @JsonRawValue
    private Integer age;
    @JsonRawValue
    private String sex;
    @JsonRawValue
    private String enrichmentField;
}
