package io.github.dflib.query.filter;

import io.github.dflib.exception.StatdException;
import io.github.dflib.exception.StorageRequestError;
import io.github.dflib.query.Query;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
public final class NumberIn implements Filter {

    private String field;

    private List<Number> values;

    private boolean negative;

    public NumberIn(String field, List<Number> values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
    }

    public NumberIn(String field, List<Number> values,boolean negative) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
        this.negative = negative;
    }

    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(StorageRequestError.InvalidFilter, "in field is null");
        }
        if (CollectionUtils.isEmpty(values)) {
            throw new StatdException(StorageRequestError.InvalidFilter, "in field value is null");
        }
    }

}
