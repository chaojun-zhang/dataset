package io.github.dflib.storage;

import io.github.dflib.dataframe.EventDataFrame;
import io.github.dflib.query.Query;
import io.github.dflib.storage.config.MultipleStorage;
import io.github.dflib.storage.config.SingleStorage;
import io.github.dflib.storage.config.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class DataFrameReader implements ApplicationContextAware {

    private ApplicationContext applicationContext;


    private StorageReader getReader(Storage storage) {
        return applicationContext.getBean(storage.getReaderClass());
    }

    public EventDataFrame load(Query search, SingleStorage storage) {
        return getReader(storage).read(search, storage);
    }

    public Map<String, EventDataFrame> load(Query search, MultipleStorage storage) {
        return getReader(storage).readMore(search, storage);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
