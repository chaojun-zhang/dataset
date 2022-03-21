package io.github.dflib.dflib;

import com.nhl.dflib.ValueMapper;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.accumulator.ValueConverter;
import com.nhl.dflib.accumulator.ValueHolder;
import com.nhl.dflib.jdbc.connector.loader.ColumnBuilder;
import io.github.dflib.dflib.accumulator.InstantAccumulator;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface InstantColumnBuilderFactory {

    static ColumnBuilder<Long> instantAccum(int pos) {
        final ValueMapper<ResultSet, Long> mapper = rs -> {
            try {
                return rs.getLong(pos);
            } catch (SQLException e) {
                throw new RuntimeException("Error performing SQL operation", e);
            }
        };
        final ValueConverter<ResultSet, Long> valueConverter = new ValueConverter<ResultSet, Long>() {
            @Override
            public void convertAndStore(ResultSet s, ValueHolder<Long> holder) {
                holder.set(mapper.map(s));
            }

            @Override
            public void convertAndStore(ResultSet s, Accumulator<Long> accumulator) {

                accumulator.add(mapper.map(s));
            }

            @Override
            public void convertAndStore(int pos, ResultSet s, Accumulator<Long> accumulator) {
                accumulator.set(pos, mapper.map(s));
            }
        };

        return new ColumnBuilder<>(valueConverter, new InstantAccumulator());
    }

}
