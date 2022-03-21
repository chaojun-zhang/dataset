package io.github.dflib.exception;

import static io.github.dflib.exception.ErrorType.INTERNAL_ERROR;

public enum StorageConfigError implements ErrorCodeSupplier {

    StorageNotFound(600,"指标定义找不到"),
    StorageDatasourceNotDefine(601,"存储数据源没有定义"),
    StorageNotDefine(602,"存储没有定义"),
    JdbcDatasourceConfigError(603,"jdbc数据源配置错误"),
    MongoDatasourceConfigError(604,"mongo数据源配置错误"),
    MongoStorageConfigError(605,"mongo存储配置错误"),
    JdbcStorageConfigError(606,"jdbc存储配置错误"),
    TimelineStorageConfigError(607,"timeline存储配置错误"),
    GranuleStorageConfigError(608,"粒度存储配置错误"),
    SchemaConfigError(609,"schema配置错误"),
    JdbcDatasourceNotFound(610,"jdbc数据源找不到"),
    MongoDatasourceNotFound(611,"mong数据源找不到"),
    StorageEventTimeNotFound(612,"eventTime字段没有指定"),
    StorageFileNotFound(613,"存储配置找不到"),
    InvalidStorageSqlTemplate(614,"sql模板渲染失败");
    private final ErrorCode errorCode;
    private String errorMsg;

    StorageConfigError(int code,String errorMsg) {
        this.errorMsg = errorMsg;
        this.errorCode = new ErrorCode(code, String.format("%s, 内部错误：%s", name(), errorMsg), INTERNAL_ERROR);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
