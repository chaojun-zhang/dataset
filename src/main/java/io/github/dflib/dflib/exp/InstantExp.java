package io.github.dflib.dflib.exp;

import com.nhl.dflib.Exp;
import io.github.dflib.dflib.column.InstantColumn;
import io.github.dflib.query.Granularity;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public interface InstantExp extends Exp<Long> {

    default InstantExp period(Granularity granularity) {
        if (Granularity.isAllGranularity(granularity)) {
            return this;
        }
        return InstantExpScalar2.mapVal("period", this, granularity, (time, g) -> {
            LocalDateTime dateTime = g.getDateTime(new Timestamp(time).toLocalDateTime());
            return Timestamp.valueOf(dateTime).getTime();
        });
    }

    static InstantExp $datetime(String name) {
        return new InstantColumn(name);
    }
}