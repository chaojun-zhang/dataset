package io.github.dflib.storage.config;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.dflib.exception.StatdException;
import io.github.dflib.exception.StorageConfigError;
import io.vavr.control.Option;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Data
@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Config {

    private Map<String, Datasource> datasource;

    private Map<String, Storage> storage;

    @JsonIgnore
    @Value("${storage.config}")
    private String configFile;

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @PostConstruct
    public void init() {
        try (InputStream configInput = this.getClass().getResourceAsStream(configFile)) {
            Config config = mapper.readValue(configInput, Config.class);
            this.datasource = config.getDatasource();
            this.storage = config.getStorage();
            this.validate();
        } catch (IOException e) {
            throw new StatdException(StorageConfigError.StorageFileNotFound, e);
        }
    }


    public void validate() throws StatdException {
        if (storage == null || storage.isEmpty()) {
            throw new StatdException(StorageConfigError.StorageNotDefine, "config storage property null");
        }
        if (datasource == null) {
            throw new StatdException(StorageConfigError.StorageDatasourceNotDefine, "config datasource property is null");
        }
        storage.values().forEach(Storage::validate);
        datasource.values().forEach(Datasource::validate);

    }

    public <T extends Storage> T getStorage(String name){
        return  (T)getStorage().get(name);
    }

    public Option<JdbcDataSource> getJdbcDataSource(String name) {
        Datasource datasource = this.getDatasource().get(name);
        if (datasource instanceof JdbcDataSource) {
            return Option.of((JdbcDataSource)datasource);
        }
        return Option.none();
    }

    public Option<MongoDataSource> getMongoDataSource(String name) {
        Datasource datasource = this.getDatasource().get(name);
        if (datasource instanceof MongoDataSource) {
            return Option.of((MongoDataSource)datasource);
        }
        return Option.none();
    }


}
