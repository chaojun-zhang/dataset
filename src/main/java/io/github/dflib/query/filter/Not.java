package io.github.dflib.query.filter;

import io.github.dflib.query.Query;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@NoArgsConstructor
public final class Not implements Filter {

    private Filter child;

    public Not(Filter child) {
        this.child = Objects.requireNonNull(child);
    }

    @Override
    public void validate(Query search) {
        Objects.requireNonNull(child, " not filter is null");
        child.validate(search);
    }

}
