package io.github.dflib.query;

import lombok.Data;

import java.util.Objects;

@Data
public class SortOrder {
    private String name;
    private boolean asc;

    public void validate(){
        Objects.requireNonNull(name, " name is required for sort");
    }

    public static SortOrder asc(String name) {
        SortOrder sortOrder = new SortOrder();
        sortOrder.setName(name);
        sortOrder.setAsc(true);
        return sortOrder;
    }

    public static SortOrder desc(String name) {
        SortOrder sortOrder = new SortOrder();
        sortOrder.setName(name);
        sortOrder.setAsc(false);
        return sortOrder;
    }
}

