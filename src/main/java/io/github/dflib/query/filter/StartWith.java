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
public final class StartWith implements LikeFilter {
    private String field;
    private String value;

    public StartWith(String field, String value) {
        this.field = Objects.requireNonNull(field);
        this.value = Objects.requireNonNull(value);

    }


    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(StorageRequestError.InvalidFilter, "start with field is null");
        }
        if (value == null) {
            throw new StatdException(StorageRequestError.InvalidFilter, "value is null");
        }
    }

    @Override
    public String getField() {
        return null;
    }
}
