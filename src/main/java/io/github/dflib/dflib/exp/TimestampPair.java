package io.github.dflib.dflib.exp;

import lombok.Data;

import java.sql.Timestamp;
import java.time.LocalDate;

@Data
public class TimestampPair {

    private Long timestamp;
    private Number value;


    public LocalDate day() {
        return new Timestamp(timestamp).toLocalDateTime().toLocalDate();
    }

}
