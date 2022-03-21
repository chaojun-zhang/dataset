package io.github.dflib.web.controller.interceptor;

import io.github.dflib.exception.StatdException;
import io.github.dflib.web.controller.ResultBody;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * 处理自定义的业务异常
     *
     * @param req
     * @param e
     * @return
     */
    @ExceptionHandler(value = StatdException.class)
    @ResponseBody
    public ResultBody exceptionHandler(HttpServletRequest req, StatdException e) {
        log.error("发生业务异常！原因是：{}", e.getMessage());
        return ResultBody.error(e.getErrorCode().getCode(), e.getErrorCode().getName());
    }

    /**
     * 处理其他异常
     */
    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public ResultBody exceptionHandler(HttpServletRequest req, Exception e) {
        log.error("未知异常！原因是:", e);
        return ResultBody.error(HttpStatus.INTERNAL_SERVER_ERROR.value(), "服务发生未知异常");
    }

}
