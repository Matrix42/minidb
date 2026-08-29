package com.minidb.tpcds;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    record Cmp(String name, long time) implements Comparable<Cmp> {

        @Override
        public int compareTo(Cmp o) {
            return Math.toIntExact(time - o.time);
        }
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("E:\\tpc-ds\\report");
        File[] jsons = file.listFiles(f -> f.getName().endsWith("json"));
        List<Cmp> list = new ArrayList<>();
        for (File json : jsons) {
            JsonNode root = mapper.readTree(json);
            JsonNode queries = root.get("queries");
            AtomicLong total = new AtomicLong();
            queries.elements()
                    .forEachRemaining(
                            n -> {
                                total.addAndGet(n.get("elapsedMs").asLong());
                            });
            list.add(new Cmp(json.getName(), total.get() / 1000));
        }
        list.sort(Cmp::compareTo);
        for (Cmp cmp : list) {
            System.out.println(cmp.name + "->" + cmp.time);
        }
    }
}
