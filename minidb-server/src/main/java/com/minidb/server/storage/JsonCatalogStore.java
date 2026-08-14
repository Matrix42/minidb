package com.minidb.server.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.server.catalog.CatalogSnapshot;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/** JSON 持久化。save 用临时文件 + move,避免崩溃写坏;并发 DDL 用 synchronized 串行化。 */
public class JsonCatalogStore implements CatalogStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;

    public JsonCatalogStore(Path file) {
        this.file = file;
    }

    @Override
    public synchronized CatalogSnapshot load() throws IOException {
        if (!Files.exists(file)) {
            return new CatalogSnapshot(List.of(), List.of());
        }
        return MAPPER.readValue(Files.readString(file, StandardCharsets.UTF_8), CatalogSnapshot.class);
    }

    @Override
    public synchronized void save(CatalogSnapshot snapshot) throws IOException {
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, MAPPER.writeValueAsString(snapshot), StandardCharsets.UTF_8);
        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
