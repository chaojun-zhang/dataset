package io.github.dflib.web.model;

import lombok.Data;

import java.util.List;

@Data
public class Table implements QueryResult {
    private List<String> fields;
    private List<Record> data;

    public int size() {
        return data.size();
    }
}
