package io.github.dflib.web.service;

import com.nhl.dflib.Index;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.exception.StatdException;
import io.github.dflib.exception.StorageConfigError;
import io.github.dflib.query.Query;
import io.github.dflib.storage.DataFrameReader;
import io.github.dflib.storage.StorageFactory;
import io.github.dflib.storage.config.MultipleStorage;
import io.github.dflib.storage.config.SingleStorage;
import io.github.dflib.storage.config.Storage;
import io.github.dflib.web.model.PipelineResult;
import io.github.dflib.web.model.QueryResult;
import io.github.dflib.web.model.Record;
import io.github.dflib.web.model.Table;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class MetricQueryService {

    private final DataFrameReader datasetReader;

    private final StorageFactory storageReaderFactory;

    @Autowired
    public MetricQueryService(DataFrameReader datasetReader, StorageFactory storageReaderFactory) {
        this.datasetReader = datasetReader;
        this.storageReaderFactory = storageReaderFactory;
    }

    public QueryResult load(Query query, String metricName) {
        query.validate();
        Storage storage = storageReaderFactory.getStorage(metricName);
        if (storage == null) {
            throw new StatdException(StorageConfigError.StorageNotDefine, "metric not found");
        }
        if (storage instanceof SingleStorage) {
            EventDataFrame dataFrame = datasetReader.load(query, (SingleStorage) storage);
            return toTable(dataFrame);
        } else {
            return pipeline(query, (MultipleStorage) storage);
        }
    }

    public PipelineResult pipeline(Query query, MultipleStorage storage) {
        Map<String, EventDataFrame> dataFrameMap = datasetReader.load(query,  storage);
        PipelineResult pipelineResult = new PipelineResult();
        dataFrameMap.forEach((k,v)-> {
            pipelineResult.getResult().put(k, toTable(v));
        });
        return pipelineResult;
    }


    public Table toTable(EventDataFrame dataFrame) {
        Index columnsIndex = dataFrame.getTable().getColumnsIndex();
        List<Record> rows = new ArrayList<>();
        dataFrame.getTable().forEach(row -> {
            Record res = new Record();
            for (String colName : columnsIndex) {
                res.put(colName, row.get(colName));
            }
            rows.add(res);
        });

        Table table = new Table();
        table.setData(rows);
        table.setFields(Arrays.asList(columnsIndex.getLabels()));
        return table;
    }
}
