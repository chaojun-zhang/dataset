package io.github.dflib.dataframe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.DoubleSeries;
import com.nhl.dflib.IntSeries;
import com.nhl.dflib.LongSeries;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import io.github.dflib.dflib.series.InstantSeries;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
public final class Schema {
    private final List<Field> fields;
    private final Map<String, Integer> nameToIndex;
    private final Map<String, Field> nameToField;

    public static Schema of(Field... fields) {
        return new Schema(Arrays.asList(fields));
    }
    public Schema(List<Field> fields) {
        this.fields = ImmutableList.copyOf(Objects.requireNonNull(fields));
        ImmutableMap.Builder<String,Integer> nameToIndex = ImmutableMap.builder();
        ImmutableMap.Builder<String,Field> nameToField = ImmutableMap.builder();
        int index = 0;
        for (Field field : fields) {
            nameToIndex.put(field.getName(), index++);
            nameToField.put(field.getName(), field);
        }
        this.nameToField = nameToField.build();
        this.nameToIndex = nameToIndex.build();
    }

    public boolean isSame(Schema schema) {
        if (fields.size() != schema.fields.size()) {
            return false;
        }
        return Arrays.equals(fields.toArray(), schema.fields.toArray());
    }


    public Optional<Field> getEventTimeField() {
        return fields.stream().filter(it -> it.getType() == FieldType.INSTANT).findFirst();
    }

    public Optional<Field> findField(String name) {
        return fields.stream().filter(it -> it.getName().equals(name)).findFirst();
    }

    public Field getCheckedField(String name) {
        return fields.stream().filter(it -> it.getName().equals(name)).findFirst().orElseThrow(() ->
                new IllegalArgumentException("field " + name + " not found"));
    }


    public List<Field> getDimensionFields() {
        return fields.stream().filter(Field::isDimensionField).collect(Collectors.toList());
    }

    public List<Field> getMetricFields() {
        return fields.stream().filter(Field::isMetricField).collect(Collectors.toList());
    }

    public String[] allColumnLabels() {
        return fields.stream().map(Field::getName).toArray(String[]::new);
    }

    public Accumulator[] accumulators() {
        return fields.stream().map(Field::accumulator).toArray(Accumulator[]::new);
    }

    public static Schema createFromDataFrame(DataFrame dataFrame) {
        List<Field> fields = new ArrayList<>();
        for (String columnsIndex : dataFrame.getColumnsIndex()) {
            Series column = dataFrame.getColumn(columnsIndex);
            if (column instanceof InstantSeries){
                fields.add(new Field(columnsIndex, FieldType.INSTANT));
            } else if (column instanceof LongSeries) {
                fields.add(new Field(columnsIndex, FieldType.LONG));
            }else if (column instanceof DoubleSeries) {
                fields.add(new Field(columnsIndex, FieldType.DOUBLE));
            }else if (column instanceof IntSeries) {
                fields.add(new Field(columnsIndex, FieldType.INT));
            }else {
                fields.add(new Field(columnsIndex, FieldType.STRING));
            }
        }
        return new Schema(fields);
    }


}
