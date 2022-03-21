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
public final class StringIn implements Filter {

    private String field;

    private List<String> values;

    private boolean negate;

    public StringIn(String field, List<String> values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
    }


    public StringIn(String field, List<String> values, boolean negate) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
        this.negate = negate;
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
