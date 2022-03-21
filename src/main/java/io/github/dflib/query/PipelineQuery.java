package io.github.dflib.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PipelineQuery {
    //查询输入
    private String input;
    //定义的输入源是不是从storage配置中查找
    private String storage;
    //查询条件
    private Query query;
    //查询结果输出
    private String output;

    @JsonIgnore
    public boolean isSource() {
        return storage != null && input == null;
    }

    @JsonIgnore
    public boolean isSink() {
        return input != null && storage == null;
    }

    public void validate() {
        if (input == null && storage == null) {
            throw new IllegalStateException("input or stroage must be provided");
        }
        //为了让校验通过，这里不需要设置interval，等在查询的时候会进行传递
        if (query == null ) {
            throw new IllegalStateException("query must be provided");
        } else {
            query.setInterval(Interval.dummy);
            query.validate();
        }

    }

}