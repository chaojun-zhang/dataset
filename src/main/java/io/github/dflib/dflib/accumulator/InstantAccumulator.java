package io.github.dflib.dflib.accumulator;

import com.nhl.dflib.accumulator.Accumulator;
import io.github.dflib.dflib.series.InstantSeries;

import java.util.Arrays;

//TODO相同时间列可以用一个值来代替就行
public class InstantAccumulator implements Accumulator<Long> {

    private long[] data;
    private int size;

    public InstantAccumulator() {
        this(10);
    }

    public InstantAccumulator(int capacity) {
        this.size = 0;
        this.data = new long[capacity];
    }

    public void fill(int from, int to, long value) {
        if (to - from < 1) {
            return;
        }

        if (data.length <= to) {
            expand(to);
        }

        Arrays.fill(data, from, to, value);
        size += to - from;
    }

    @Override
    public void add(Long v) {
        addLong(v != null ? v : 0L);
    }

    @Override
    public void addLong(long value) {

        if (size == data.length) {
            expand(data.length * 2);
        }

        data[size++] = value;
    }

    @Override
    public void set(int pos, Long v) {
        setLong(pos, v != null ? v : 0L);
    }

    @Override
    public void setLong(int pos, long value) {

        if (pos >= size) {
            throw new IndexOutOfBoundsException(pos + " is out of bounds for " + size);
        }

        data[pos] = value;
    }

    @Override
    public InstantSeries toSeries() {
        long[] data = compactData();

        // making sure no one can change the series via the Mutable List anymore
        this.data = null;

        return new InstantSeries(data, 0, size);
    }

    public int size() {
        return size;
    }

    private long[] compactData() {
        if (data.length == size) {
            return data;
        }

        long[] newData = new long[size];
        System.arraycopy(data, 0, newData, 0, size);
        return newData;
    }

    private void expand(int newCapacity) {
        long[] newData = new long[newCapacity];
        System.arraycopy(data, 0, newData, 0, size);

        this.data = newData;
    }

}
