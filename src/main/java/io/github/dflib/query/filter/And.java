package io.github.dflib.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.dflib.query.Query;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
public final class And implements LogicalFilter {

    private Filter left;
    private Filter right;

    public And(Filter left,Filter right) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    public void validate(Query search) {
        Objects.requireNonNull(left, " left filter of and is null");
        Objects.requireNonNull(left, " right filter of and is null");
        left.validate(search);
        right.validate(search);
    }



}
