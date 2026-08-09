package com.minidb.server;

import java.nio.file.Path;

public final class Main {
    public static void main(String[] args) throws Exception {
        int port = 8899;
        Path dataDir = Path.of("data");
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i])) {
                port = Integer.parseInt(args[++i]);
            } else if ("--data".equals(args[i])) {
                dataDir = Path.of(args[++i]);
            }
        }
        MiniDbServer server = new MiniDbServer();
        server.start(port, dataDir);
        System.out.println("MiniDB listening on port " + server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
