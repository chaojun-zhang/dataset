package io.github.dflib.web.controller;

import lombok.Data;
import org.springframework.http.HttpStatus;

@Data
public class ResultBody {

    private Object data;

    private int code;

    private String message;

    public static ResultBody error(int code, String message) {
        ResultBody result = new ResultBody();
        result.setMessage(message);
        result.setCode(code);
        return result;
    }

    public static ResultBody ok(Object body) {
        ResultBody result = new ResultBody();
        result.setData(body);
        result.setCode(HttpStatus.OK.value());
        result.setMessage("OK");
        return result;
    }
}
