package io.github.dflib.exception;


public enum StorageRequestError implements ErrorCodeSupplier {
    //无效时间格式
    InvalidTimeFormat(700,"无效时间格式"),
    //无效的时间粒度
    InvalidGranule(701,"无效的粒度"),
    //topn limit参数错误
    InvalidTopN(702,"无效topn"),
    //无效的时间范围
    InvalidInterval(703,"无效的时间范围"),
    //无效的查询字段
    InvalidQueryField(704,"无效查询字段"),

    //无需的查询值
    InvalidQueryValue(705,"无效的维度值"),

    //维度字段找不到
    DimensionFieldNotFound(706,"维度字段找不到"),

    InvalidDimensions(707,"无效的维度"),

    //指标字段找不到
    MetricFieldNotFound(708,"指标字段找不到"),
    InvalidMetrics(709,"无效的指标"),
    InvalidFilter(710,"无效的过滤条件"),
    InvalidQueryType(711,"查询类型不支持"),
    InvalidQueryPipeline(712,"无效的查询管道"),
    InvalidQuery(713,"查询无效");

    private final ErrorCode errorCode;
    private final String errorMsg;

    StorageRequestError(int code, String errorMsg) {
        this.errorMsg = errorMsg;
        this.errorCode = new ErrorCode(code,
                String.format("%s,%s", "badRequest", errorMsg),
                ErrorType.USER_ERROR);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
