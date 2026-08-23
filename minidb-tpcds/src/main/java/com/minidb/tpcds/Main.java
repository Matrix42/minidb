package com.minidb.tpcds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File("E:\\tpc-ds\\report\\7.存储文件列剪裁2.json"));
        //root = mapper.readTree(new File("E:\\tpc-ds\\report\\6.公共子表达式消除(CSE).json"));
        JsonNode queries = root.get("queries");
        AtomicLong total = new AtomicLong();
        queries.elements().forEachRemaining(n -> {
            total.addAndGet(n.get("elapsedMs").asLong());
        });
        System.out.println(total.get() / 1000);
        System.out.println(total.get() / 1000 / 60);

    }

}
