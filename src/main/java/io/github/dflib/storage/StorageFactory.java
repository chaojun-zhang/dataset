package io.github.dflib.storage;

import io.github.dflib.storage.config.Config;
import io.github.dflib.storage.config.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorageFactory {

    @Autowired
    private Config config;

    public <T extends Storage> T getStorage(String storageName) {
        Storage storage = config.getStorage().get(storageName);
        if (storage == null) {
            throw new IllegalArgumentException(String.format("storage not found for '%s'", storageName));
        }
        return (T) storage;
    }

}
