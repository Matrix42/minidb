package com.minidb.server.exec;

import com.minidb.server.catalog.MiniDbCatalog;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class MetadataExecutor {

    private static final ArrowType VARCHAR = ArrowType.Utf8.INSTANCE;

    private final MiniDbCatalog catalog;
    private final BufferAllocator allocator;

    public MetadataExecutor(MiniDbCatalog catalog, BufferAllocator allocator) {
        this.catalog = catalog;
        this.allocator = allocator;
    }

    public VectorSchemaRoot schemas(String schemaPattern) {
        Pattern like = compileLike(schemaPattern);
        List<String> matched = new ArrayList<>();
        for (String s : catalog.schemaNames()) {
            if (like == null || like.matcher(s).matches()) {
                matched.add(s);
            }
        }
        matched.sort(String::compareTo);
        VarCharVector schem = new VarCharVector("TABLE_SCHEM", allocator);
        VarCharVector cat = new VarCharVector("TABLE_CAT", allocator);
        schem.setInitialCapacity(matched.size());
        cat.setInitialCapacity(matched.size());
        schem.allocateNew();
        cat.allocateNew();
        for (int i = 0; i < matched.size(); i++) {
            schem.setSafe(i, matched.get(i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        schem.setValueCount(matched.size());
        cat.setValueCount(matched.size());
        return VectorSchemaRoot.of(schem, cat);
    }

    static Pattern compileLike(String pattern) {
        if (pattern == null) return null;
        StringBuilder sb = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return Pattern.compile(sb.toString(), Pattern.DOTALL);
    }
}
