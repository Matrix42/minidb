package com.minidb.server;

import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

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
        LOG.info("MiniDB starting on port {} with data dir {}", port, dataDir);
        MiniDbServer server = new MiniDbServer();
        server.start(port, dataDir);
        LOG.info("MiniDB listening on port {}", server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("MiniDB shutting down");
            server.close();
        }));
        Thread.currentThread().join();
    }
}
