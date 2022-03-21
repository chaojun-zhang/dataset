package io.github.dflib.storage;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.DataFrameBuilder;
import com.nhl.dflib.DataFrameByRowBuilder;
import com.nhl.dflib.row.RowProxy;
import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.exception.StatdException;
import io.github.dflib.exception.StorageConfigError;
import io.github.dflib.exception.StorageRequestError;
import io.github.dflib.query.Granularity;
import io.github.dflib.query.Query;
import io.github.dflib.storage.config.TimelineStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

/**
 * 实时结果放在实时表，离线结果放在离线表，分开存储需要进行合并
 */
@Component
public class TimelineReader implements StorageReader<TimelineStorage> {

    private final DataFrameReader datasetReader;

    @Autowired
    public TimelineReader(DataFrameReader datasetReader) {
        this.datasetReader = datasetReader;
    }

    @Override
    public EventDataFrame read(Query query, TimelineStorage storage) throws StatdException {
        if (storage.getBatch() != null && storage.getStream() == null) {
            return datasetReader.load(query, storage.getBatch());
        } else if (storage.getBatch() == null && storage.getStream() != null) {
            return datasetReader.load(query, storage.getStream());
        } else {
            if (Granularity.isAllGranularity(query.getGranularity())) {
                throw new StatdException(StorageRequestError.InvalidGranule, "Error while time line read, query's granularity is required");
            }

            EventDataFrame batchDataFrame = datasetReader.load(query, storage.getBatch());
            EventDataFrame streamBatchFrame = datasetReader.load(query, storage.getStream());
            if (!batchDataFrame.getSchema().isSame(streamBatchFrame.getSchema())) {
                throw new StatdException(StorageConfigError.SchemaConfigError, "batchDataFrame's schema must be  same as streamDataFrame's schema");
            }
            return merge(batchDataFrame, streamBatchFrame);
        }
    }

    /**
     * 排序后进行合并
     */
    private EventDataFrame merge(EventDataFrame batch ,EventDataFrame stream){
        if (batch.getTable().height() == 0) {
            return stream;
        }
        if (stream.getTable().height() == 0) {
            return batch;
        }

        final DataFrame sortedBatch =  batch.getTable().sort(batch.getSchema().getEventTimeField().get().getName(), true);
        final DataFrame sortedStream = stream.getTable().sort(batch.getSchema().getEventTimeField().get().getName(), true);

        final java.util.Iterator<RowProxy> batchIterator = sortedBatch.iterator();
        final java.util.Iterator<RowProxy> streamIterator = sortedStream.iterator();

        RowProxy batchRow = batchIterator.next();
        RowProxy streamRow = streamIterator.next();

        DataFrameByRowBuilder dataFrameByRowBuilder = DataFrameBuilder.builder(batch.getTable().getColumnsIndex()).byRow(batch.getSchema().accumulators());
        while (batchRow!= null || streamRow!= null) {
            if (streamRow == null) {
                dataFrameByRowBuilder.addRow(toRow(batchRow));
                batchRow = batchIterator.hasNext()? batchIterator.next(): null;
            } else if (batchRow == null) {
                dataFrameByRowBuilder.addRow(toRow(streamRow));
                streamRow = streamIterator.hasNext()? streamIterator.next(): null;
            }else {
                Timestamp batchTimestamp = new Timestamp((long)batchRow.get(batch.getSchema().getEventTimeField().get().getName()));
                Timestamp streamTimestamp =  new Timestamp((long)streamRow.get(stream.getSchema().getEventTimeField().get().getName()));
                if (batchTimestamp.getTime() > streamTimestamp.getTime()) {
                    dataFrameByRowBuilder.addRow(toRow(streamRow));
                    streamRow = streamIterator.hasNext()? streamIterator.next(): null;
                }else if (batchTimestamp.getTime() < streamTimestamp.getTime()){
                    dataFrameByRowBuilder.addRow(toRow(batchRow));
                    batchRow = batchIterator.hasNext()? batchIterator.next(): null;
                }else {
                    dataFrameByRowBuilder.addRow(toRow(batchRow));
                    streamRow = streamIterator.hasNext()? streamIterator.next(): null;
                    batchRow = batchIterator.hasNext()? batchIterator.next(): null;
                }
            }
        }
        return batch.withTable(dataFrameByRowBuilder.create());
    }

    private Object[] toRow(RowProxy rowProxy) {
        Object[] row = new Object[rowProxy.getIndex().size()];
        for (int i = 0; i < rowProxy.getIndex().size(); i++) {
            row[i] = rowProxy.get(i);
        }
        return row;
    }


}