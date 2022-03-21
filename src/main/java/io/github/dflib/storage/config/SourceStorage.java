package io.github.dflib.storage.config;

import io.github.dflib.query.Granularity;

public interface SourceStorage extends SingleStorage {

    /**
     * 底层存储的时间字段
     * @return
     */
    String getEventTimeField();

    /**
     * 底层存储的时间粒度
     * @return
     */
    Granularity getGranularity();

 }
