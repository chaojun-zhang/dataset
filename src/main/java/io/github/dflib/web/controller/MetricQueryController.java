package io.github.dflib.web.controller;


import io.github.dflib.exception.StatdException;
import io.github.dflib.exception.StorageRequestError;
import io.github.dflib.query.Query;
import io.github.dflib.web.service.MetricQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class MetricQueryController {

    private final MetricQueryService metricService;

    @Autowired
    public MetricQueryController(MetricQueryService metricService) {
        this.metricService = metricService;
    }


    @PostMapping(value = "/v1/query/{metric}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResultBody query(@PathVariable(name = "metric") String metric, @RequestBody Query query) {
        if (query == null) {
            throw new StatdException(StorageRequestError.InvalidQuery, "query is null");
        }
        log.info("query metric: {}, req: {}", metric, query);
        return ResultBody.ok(metricService.load(query, metric));
    }


}
