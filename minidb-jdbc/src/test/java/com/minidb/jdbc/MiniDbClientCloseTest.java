package com.minidb.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** MiniDbClient 释放兜底(bug #28):close 幂等、可被 finalize 安全调用。 */
class MiniDbClientCloseTest {

    @Test
    void closeIsIdempotent() {
        MiniDbClient client = new MiniDbClient();
        // 未 connect 也可安全 close(释放本地 group/allocator)。
        assertDoesNotThrow(client::close);
        // 二次 close 不得抛异常(finalize 路径的本质要求)。
        assertDoesNotThrow(client::close);
    }
}
