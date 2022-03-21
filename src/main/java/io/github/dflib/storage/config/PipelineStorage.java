package io.github.dflib.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.SortOrder;
import io.github.dflib.storage.StorageReader;
import io.github.dflib.storage.pipeline.PipelineReader;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PipelineStorage implements MultipleStorage {

    private Map<String, String> params = new HashMap<>();
    private Map<String, String> functions = new HashMap<>();
    private Map<String, SingleStorage> input = new HashMap<>();
    private MultipleStorage multipleStorage;
    private List<Transform> steps = new ArrayList<>();
    private List<Output> output = new ArrayList<>();

    @JsonIgnore
    @Override
    public Class<? extends StorageReader> getReaderClass() {
        return PipelineReader.class;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Window.class, name = "window"),
            @JsonSubTypes.Type(value = Project.class, name = "project"),
            @JsonSubTypes.Type(value = Limit.class, name = "limit"),
            @JsonSubTypes.Type(value = Filter.class, name = "filter"),
            @JsonSubTypes.Type(value = Sorter.class, name = "sort"),
            @JsonSubTypes.Type(value = Join.class, name = "join"),
            @JsonSubTypes.Type(value = Summarize.class, name = "summarize")
    })
    public static class Transform {
        private String dataFrameName;

        public void validate() {
            Objects.requireNonNull(dataFrameName, "step dataFrameName is required");
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract static class OneInputTransform extends Transform {
        private String input;

        public void validate() {
            super.validate();
            Objects.requireNonNull(input, "input is required for oneInput step");
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract static class TwoInputTransform extends Transform {

        private String left;
        private String right;

        public void validate() {
            Objects.requireNonNull(left, "left is required for twoInput step");
            Objects.requireNonNull(right, "right is required  for twoInput step");
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Join extends TwoInputTransform {
        private JoinType joinType;
        private String leftColumn;
        private String rightColumn;

        public void validate() {
            Objects.requireNonNull(joinType, "joinType is required for Join step");
            Objects.requireNonNull(leftColumn, "leftColumn is required for Join step");
            Objects.requireNonNull(rightColumn, "rightColumn is required  for Join step");

        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Project extends OneInputTransform {
        private String[] projects;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Filter extends OneInputTransform {
        private String filter;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Sorter extends OneInputTransform {
        private String[] orders;

        public List<SortOrder> sortOrders() {
            return PipelineStorage.sortOrders(orders);
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Limit extends OneInputTransform {
        private int limit;
    }

    public enum JoinType {
        LEFT, RIGHT, INNER
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Summarize extends OneInputTransform {
        private String[] grouping;
        private String[] metrics;
        private Granularity granularity;
        public void validate() {
            super.validate();
            Preconditions.checkState(metrics != null && metrics.length > 0, "metrics is required for group by step");

        }

    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Window extends OneInputTransform {
        private String[] partitions;
        private String[] orders;
        private WindowFunction windowFunction;
        private String name;

        public List<SortOrder> sortOrders() {
            return PipelineStorage.sortOrders(orders);
        }

    }

    public enum WindowFunction {
        number, rank, denseRank;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Output {
        private String dataFrameName;
        private boolean timeseries;
        private String name;

        public void validate() {
            Objects.requireNonNull(dataFrameName);
        }

    }

    @Override
    public void validate() {
        if (input == null || input.isEmpty()) {
            throw new IllegalStateException("pipeline input required");
        } else {
            input.forEach((name, storage) -> storage.validate());
        }

        if (steps != null && !steps.isEmpty()) {
            steps.forEach(Transform::validate);
        }

        if (output == null || output.isEmpty()) {
            throw new IllegalStateException("at least one pipeline output required");
        } else {
            output.forEach(Output::validate);
        }

    }


    public static List<SortOrder> sortOrders(String[] orders) {
        if (orders != null && orders.length>0) {
            final List<SortOrder> sortOrders = new ArrayList<>();
            Arrays.asList(orders).forEach(it -> {
                boolean isAsc = !it.startsWith("-");
                String fieldName = it;
                if (it.startsWith("+") || it.startsWith("-")) {
                    fieldName = it.substring(1);
                }
                SortOrder sortOrder = new SortOrder();
                sortOrder.setAsc(isAsc);
                sortOrder.setName(fieldName);
                sortOrders.add(sortOrder);
            });
            return sortOrders;
        } else {
            return new ArrayList<>();
        }

    }
}
