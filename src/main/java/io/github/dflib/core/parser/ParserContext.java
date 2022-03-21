package io.github.dflib.core.parser;

import io.github.dflib.dataframe.Schema;
import io.github.dflib.query.Interval;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ParserContext {

    private Map<String, String> functions = new HashMap<>();

    private Map<String, String> params = new HashMap<>();

    private Interval interval;

    private Schema schema;

    public String getParam(String key) {
        return this.params.get(key);
    }

    public void addParam(String key, String value) {
        this.params.put(key, value);
    }

    public void registerFunction(String key, Class value) {
        this.functions.put(key, value.getCanonicalName());
    }
}
