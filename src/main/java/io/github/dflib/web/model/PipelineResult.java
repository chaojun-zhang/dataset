package io.github.dflib.web.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;


@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PipelineResult implements QueryResult {

    private Map<String, Table> result = new HashMap<>();


}
