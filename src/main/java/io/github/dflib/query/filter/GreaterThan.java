package io.github.dflib.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.dflib.exception.StatdException;
import io.github.dflib.exception.StorageRequestError;
import io.github.dflib.query.Query;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@NoArgsConstructor
public final class GreaterThan implements FieldFilter {
    private String field;
    private Number value;

    public GreaterThan(String field, Number value) {
        this.field = Objects.requireNonNull(field);
        this.value = Objects.requireNonNull(value);

    }


    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(StorageRequestError.InvalidFilter, "gt field is null");
        }
        if (value == null) {
            throw new StatdException(StorageRequestError.InvalidFilter, "gt value is null");
        }
    }

}
