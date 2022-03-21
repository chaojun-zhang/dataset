package io.github.dflib.storage.pipeline;

import com.nhl.dflib.Exp;
import com.nhl.dflib.Hasher;
import io.github.dflib.core.parser.ParserContext;
import io.github.dflib.core.parser.exp.ExpCompiler;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.dataframe.Schema;
import io.github.dflib.query.Granularity;
import io.github.dflib.storage.config.PipelineStorage;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.nhl.dflib.Exp.$str;
import static io.github.dflib.dflib.exp.InstantExp.$datetime;

public final class AggregationBuilder {

    private final EventDataFrame dataFrame;
    private final PipelineStorage.Summarize summarize;

    public AggregationBuilder(EventDataFrame dataFrame, PipelineStorage.Summarize summarize) {
        this.dataFrame = dataFrame;
        this.summarize = summarize;
    }

    public Optional<Hasher> buildHasher() {
        Schema schema = dataFrame.getSchema();
        Hasher hasher = null;

        if (schema.getEventTimeField().isPresent() && !Granularity.isAllGranularity(summarize.getGranularity())) {
            hasher = (r) -> {
                Timestamp timestamp = new Timestamp((Long) r.get(schema.getEventTimeField().get().getName()));
                return summarize.getGranularity().getDateTime(timestamp.toLocalDateTime());
            };
        }
        if (summarize.getGrouping() != null) {
            for (String dimension : summarize.getGrouping()) {
                if (hasher == null) {
                    hasher = Hasher.forColumn(dimension);
                } else {
                    hasher = hasher.and(dimension);
                }
            }
        }
        return Optional.ofNullable(hasher);
    }

    public Exp<?>[] buildAggregators(ParserContext context) {
        final List<Exp<?>> result = new ArrayList<>();
        if (!Granularity.isAllGranularity(summarize.getGranularity())) {
            Optional<Exp<Long>> eventTimeFirstAgg = dataFrame.getSchema().getEventTimeField()
                    .map(it -> $datetime(it.getName()).period(summarize.getGranularity()).first().as(it.getName()));
            eventTimeFirstAgg.ifPresent(result::add);
        }

        if (summarize.getGrouping() != null ) {
            for (String dimension : summarize.getGrouping()) {
                Exp<?> group = ExpCompiler.compile(dimension, context);
                result.add(group.first().as(dimension));
            }
        }
        List<Exp<?>> metricExprs = Arrays.stream(summarize.getMetrics()).map(it -> ExpCompiler.compile(it, context)).collect(Collectors.toList());
        result.addAll(metricExprs);
        return result.toArray(new Exp<?>[0]);
    }

}
