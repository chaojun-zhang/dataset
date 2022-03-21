package io.github.dflib.storage.pipeline;

import com.nhl.dflib.Condition;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Exp;
import com.nhl.dflib.Hasher;
import com.nhl.dflib.JoinType;
import com.nhl.dflib.Sorter;
import com.nhl.dflib.exp.sort.ExpSorter;
import com.nhl.dflib.join.JoinBuilder;
import com.nhl.dflib.window.WindowBuilder;
import io.github.dflib.storage.DataFrameReader;
import io.github.dflib.core.parser.CompileException;
import io.github.dflib.core.parser.ParserContext;
import io.github.dflib.core.parser.exp.ExpCompiler;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.exception.StatdException;
import io.github.dflib.query.Query;
import io.github.dflib.query.SortOrder;
import io.github.dflib.storage.StorageReader;
import io.github.dflib.storage.config.PipelineStorage;
import io.github.dflib.storage.config.SingleStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Component
@Lazy
public class PipelineReader implements StorageReader<PipelineStorage> {

    private final DataFrameReader datasetReader;


    @Autowired
    public PipelineReader(DataFrameReader datasetReader) {
        this.datasetReader = datasetReader;
    }


    @Override
    public EventDataFrame read(Query query, PipelineStorage storage) throws StatdException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, EventDataFrame> readMore(Query query, PipelineStorage pipelineStorage) {
        ParserContext compileContext = createCompileContext(query, pipelineStorage);
        Map<String, EventDataFrame> inputDataFrames = loadInputs(query, pipelineStorage);
        for (PipelineStorage.Transform step : pipelineStorage.getSteps()) {
            if (step instanceof PipelineStorage.Window) {
                execWindow((PipelineStorage.Window) step, inputDataFrames);
            } else if (step instanceof PipelineStorage.Summarize) {
                execSummarize(compileContext, (PipelineStorage.Summarize) step, inputDataFrames);
            } else if (step instanceof PipelineStorage.Project) {
                execProject(compileContext, (PipelineStorage.Project) step, inputDataFrames);
            } else if (step instanceof PipelineStorage.Filter) {
                execFilter(compileContext, (PipelineStorage.Filter) step, inputDataFrames);
            } else if (step instanceof PipelineStorage.Limit) {
                execLimit((PipelineStorage.Limit) step, inputDataFrames);
            } else if (step instanceof PipelineStorage.Sorter) {
                execSorter((PipelineStorage.Sorter) step, inputDataFrames);
            } else if (step instanceof PipelineStorage.Join) {
                execJoin((PipelineStorage.Join) step, inputDataFrames);
            }
        }
        return buildResult(inputDataFrames, pipelineStorage);
    }

    private ParserContext createCompileContext(Query query, PipelineStorage pipelineStorage) {
        ParserContext compileContext = new ParserContext();
        compileContext.setInterval(query.getInterval());
        compileContext.setFunctions(pipelineStorage.getFunctions());
        compileContext.setParams(pipelineStorage.getParams());
        return compileContext;
    }

    private Map<String, EventDataFrame> loadInputs(Query query, PipelineStorage pipelineStorage) {
        Map<String, EventDataFrame> result = new HashMap<>();
        for (Map.Entry<String, SingleStorage> nameToEventStorage : pipelineStorage.getInput().entrySet()) {
            EventDataFrame eventDataFrame = datasetReader.load(query, nameToEventStorage.getValue());
            result.put(nameToEventStorage.getKey(), eventDataFrame);
        }
        if (pipelineStorage.getMultipleStorage() != null) {
            Map<String, EventDataFrame> dataFrameMap = datasetReader.load(query, pipelineStorage.getMultipleStorage());
            result.putAll(dataFrameMap);
        }
        return result;
    }

    private void execJoin(PipelineStorage.Join join, Map<String, EventDataFrame> inputDataFrames) {
        EventDataFrame leftEventDF = inputDataFrames.get(join.getLeft());
        EventDataFrame rightEventDF = inputDataFrames.get(join.getRight());
        JoinType joinType;
        switch (join.getJoinType()) {
            case LEFT:
                joinType = JoinType.left;
                break;
            case RIGHT:
                joinType = JoinType.right;
                break;
            case INNER:
                joinType = JoinType.inner;
                break;
            default:
                joinType = JoinType.full;
        }
        DataFrame joinedDF = new JoinBuilder(leftEventDF.getTable()).type(joinType)
                .on(join.getLeftColumn(), join.getRightColumn())
                .with(rightEventDF.getTable());

        inputDataFrames.put(join.getDataFrameName(), leftEventDF.withTable(joinedDF));
    }

    private void execSorter(PipelineStorage.Sorter sorter, Map<String, EventDataFrame> inputDataFrames) {
        EventDataFrame eventDataFrame = inputDataFrames.get(sorter.getInput());
        List<SortOrder> orders = sorter.sortOrders();
        Sorter[] sorters = orders.stream()
                .map(it -> new ExpSorter(Exp.$str(it.getName()), it.isAsc()))
                .toArray(Sorter[]::new);
        DataFrame sortedDF = eventDataFrame.getTable().sort(sorters);
        inputDataFrames.put(sorter.getDataFrameName(), eventDataFrame.withTable(sortedDF));
    }

    private void execLimit(PipelineStorage.Limit limit, Map<String, EventDataFrame> inputDataFrames) {
        EventDataFrame eventDataFrame = inputDataFrames.get(limit.getInput());
        DataFrame limitDF = eventDataFrame.getTable().head(limit.getLimit());
        inputDataFrames.put(limit.getDataFrameName(), eventDataFrame.withTable(limitDF));
    }

    private void execFilter(ParserContext compileContext, PipelineStorage.Filter filter
            , Map<String, EventDataFrame> inputDataFrames) {
        Objects.requireNonNull(filter.getFilter());
        EventDataFrame dataFrame = inputDataFrames.get(filter.getInput());
        compileContext.setSchema(dataFrame.getSchema());
        Exp<?> condition = ExpCompiler.compile(filter.getFilter(), compileContext);
        if (condition instanceof Condition) {
            DataFrame filterDF = dataFrame.getTable().selectRows((Condition) condition);
            inputDataFrames.put(filter.getDataFrameName(), dataFrame.withTable(filterDF));
        } else {
            throw new CompileException("expression '" + filter.getFilter() + "' is not a valid condition");
        }
    }

    private void execProject(ParserContext context, PipelineStorage.Project project, Map<String, EventDataFrame> inputDataFrames) {
        EventDataFrame dataFrame = inputDataFrames.get(project.getInput());
        context.setSchema(dataFrame.getSchema());
        io.vavr.collection.List<Exp<?>> exps = io.vavr.collection.List.of(project.getProjects())
                .map(it -> ExpCompiler.compile(it, context));
        if (exps.size() == 1) {
            DataFrame df = dataFrame.getTable().selectColumns(exps.head());
            inputDataFrames.put(project.getDataFrameName(), dataFrame.withTable(df));
        } else {
            DataFrame df = dataFrame.getTable().selectColumns(exps.head(), exps.tail().toJavaArray(Exp[]::new));
            inputDataFrames.put(project.getDataFrameName(), dataFrame.withTable(df));
        }
    }

    public void execSummarize(ParserContext compileContext, PipelineStorage.Summarize summarize, Map<String, EventDataFrame> inputDataFrames) {
        EventDataFrame eventDataFrame = inputDataFrames.get(summarize.getInput());
        compileContext.setSchema(eventDataFrame.getSchema());

        AggregationBuilder aggregationExpBuilder = new AggregationBuilder(eventDataFrame, summarize);
        Exp<?>[] aggs = aggregationExpBuilder.buildAggregators(compileContext);
        Optional<Hasher> hasher = aggregationExpBuilder.buildHasher();
        if (hasher.isPresent()) {
            DataFrame agg = eventDataFrame.getTable().group(hasher.get()).agg(aggs);
            inputDataFrames.put(summarize.getDataFrameName(), eventDataFrame.withTable(agg));
        } else {
            DataFrame agg = eventDataFrame.getTable().agg(aggs);
            inputDataFrames.put(summarize.getDataFrameName(), eventDataFrame.withTable(agg));
        }
    }


    private void execWindow(PipelineStorage.Window window, Map<String, EventDataFrame> inputDataFrames) {
        EventDataFrame eventDataFrame = inputDataFrames.get(window.getInput());
        DataFrame table = eventDataFrame.getTable();
        Sorter[] sorters = window.sortOrders().stream()
                .map(it -> new ExpSorter(Exp.$str(it.getName()), it.isAsc()))
                .toArray(Sorter[]::new);

        WindowBuilder windowBuilder;
        if (window.getPartitions()!= null && window.getPartitions().length>0) {
            windowBuilder = table.over().partitioned(window.getPartitions()).sorted(sorters);
        } else {
            windowBuilder = table.over().sorted(sorters);
        }
        if (window.getWindowFunction() == PipelineStorage.WindowFunction.denseRank) {
            table = table.addColumn(window.getName(), windowBuilder.denseRank());
        } else if (window.getWindowFunction() == PipelineStorage.WindowFunction.rank) {
            table = table.addColumn(window.getName(), windowBuilder.rank());
        } else {
            table = table.addColumn(window.getName(), windowBuilder.rowNumber());
        }
        inputDataFrames.put(window.getDataFrameName(), eventDataFrame.withTable(table));
    }


    private Map<String, EventDataFrame> buildResult(Map<String, EventDataFrame> inputs, PipelineStorage pipelineStorage) {
        Map<String, EventDataFrame> result = new HashMap<>();
        for (PipelineStorage.Output output : pipelineStorage.getOutput()) {
            String dataFrameName = output.getDataFrameName();
            EventDataFrame dataFrame = inputs.get(dataFrameName);
            if (output.isTimeseries()) {
                result.put(output.getName(), dataFrame.toZeroDf());
            } else {
                result.put(output.getName(), dataFrame);
            }
        }
        return result;
    }


}
