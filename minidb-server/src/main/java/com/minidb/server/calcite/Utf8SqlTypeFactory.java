package com.minidb.server.calcite;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * {@link SqlTypeFactoryImpl} whose default character set is UTF-8.
 *
 * <p>Calcite defaults character string literals to ISO-8859-1 (system property {@code
 * calcite.default.charset}); a literal that cannot be encoded in that set (e.g. Chinese) is
 * rejected during conversion with "Failed to encode ... in character set 'ISO-8859-1'". MiniDB
 * stores VARCHAR as UTF-8 bytes (Arrow VarChar), so UTF-8 is the correct default here.
 */
public class Utf8SqlTypeFactory extends SqlTypeFactoryImpl {

    public Utf8SqlTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public Charset getDefaultCharset() {
        return StandardCharsets.UTF_8;
    }
}
