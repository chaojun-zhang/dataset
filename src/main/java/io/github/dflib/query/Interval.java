package io.github.dflib.query;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
public final class Interval {

    public static final Interval dummy = new Interval(LocalDateTime.MIN, LocalDateTime.MAX);

    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private LocalDateTime from;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private LocalDateTime to;

    public Interval(LocalDateTime from, LocalDateTime to) {
        this.from = Objects.requireNonNull(from);
        this.to = Objects.requireNonNull(to);
        this.validate();
    }

    public void validate(){
        Objects.requireNonNull(from, " from date is null");
        Objects.requireNonNull(to, " to date is null");
        if (from.isEqual(to) || from.isAfter(to)) {
            throw new IllegalArgumentException("to date must be after than from date");
        }
    }

    /**
     * 按照粒度获取总的slot
     */
    public int slots(Granularity granularity) {
        LocalDateTime start = from;
        int slots = 0;
        while (start.isBefore(to)) {
            slots += 1;
            start = granularity.nextTime(start);
        }
        return slots;
    }


    private List<Interval> intervals(Granularity granularity) {
        Objects.requireNonNull(granularity);
        LocalDateTime start = granularity.getDateTime(from);
        List<Interval> result = new ArrayList<>();
        while (start.isBefore(to)) {
            Interval interval = new Interval(start, granularity.nextTime(start));
            result.add(interval);
            start = granularity.nextTime(start);
        }
        return result;
    }

    public List<Interval> days() {
        return intervals(Granularity.GDay);
    }

    public static void main(String[] args) throws JsonProcessingException {
        Interval interval = new Interval(LocalDateTime.of(2022, 12, 12, 10, 10, 0), LocalDateTime.of(2025, 12, 12, 10, 10, 0));
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        System.out.println(objectMapper.writeValueAsString(interval));
    }


}
