package io.github.dflib.query.filter;

import io.github.dflib.query.Query;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public final class Or implements LogicalFilter {

    private Filter left;
    private Filter right;

    public Or(Filter left,Filter right) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    public void validate(Query search) {
        Objects.requireNonNull(left, " left filter of or is null");
        Objects.requireNonNull(left, " right filter of or is null");
        left.validate(search);
        right.validate(search);
    }


}
