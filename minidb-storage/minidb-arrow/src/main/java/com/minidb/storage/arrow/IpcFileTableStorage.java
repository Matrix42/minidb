package com.minidb.storage.arrow;

import com.minidb.storage.common.TableStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Arrow IPC 文件存储引擎:每表一个目录 {@code data/<schema>/<table>/},目录(可嵌套)里是 part 文件(.arrow)。part 读写由 {@link
 * ArrowTable} 负责,本类只管目录定位与删除。
 */
public class IpcFileTableStorage implements TableStorage {

    private final Path dataDir;

    public IpcFileTableStorage(Path dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    public List<TableRef> listTables() {
        List<TableRef> refs = new ArrayList<>();
        if (!Files.exists(dataDir)) {
            return refs;
        }
        try (DirectoryStream<Path> schemaDirs = Files.newDirectoryStream(dataDir)) {
            for (Path schemaDir : schemaDirs) {
                if (!Files.isDirectory(schemaDir)) {
                    continue;
                }
                String schemaName = schemaDir.getFileName().toString();
                try (DirectoryStream<Path> tableDirs = Files.newDirectoryStream(schemaDir)) {
                    for (Path tableDir : tableDirs) {
                        if (Files.isDirectory(tableDir)) {
                            refs.add(new TableRef(schemaName, tableDir.getFileName().toString()));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return refs;
    }

    @Override
    public Path tableDir(String schemaName, String tableName) {
        return dataDir.resolve(key(schemaName)).resolve(key(tableName));
    }

    @Override
    public void delete(String schemaName, String tableName) {
        deleteRecursively(tableDir(schemaName, tableName));
    }

    @Override
    public void deleteSchema(String schemaName) {
        deleteRecursively(dataDir.resolve(key(schemaName)));
    }

    private static void deleteRecursively(Path dir) {
        try {
            if (!Files.exists(dir)) {
                return;
            }
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
                for (Path p : ds) {
                    if (Files.isDirectory(p)) {
                        deleteRecursively(p);
                    } else {
                        Files.deleteIfExists(p);
                    }
                }
            }
            Files.deleteIfExists(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
