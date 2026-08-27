package com.minidb.server;

import com.minidb.protocol.MessageDecoder;
import com.minidb.protocol.MessageEncoder;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.config.MiniDbConfig;
import com.minidb.server.exec.MetadataExecutor;
import com.minidb.server.exec.QueryExecutor;
import com.minidb.server.netty.SessionHandler;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniDbServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MiniDbServer.class);

    private final MiniDbCatalog catalog = new MiniDbCatalog();
    private BufferAllocator allocator;
    private StorageManager storage;
    private EventLoopGroup boss;
    private EventLoopGroup workers;
    private Channel channel;
    private ExecutorService queryPool;

    public void start(int port, Path dataDir) throws Exception {
        // 向后兼容:配置仍从数据目录读(旧行为)。
        start(port, dataDir, dataDir);
    }

    public void start(int port, Path dataDir, Path confDir) throws Exception {
        allocator = new RootAllocator();
        storage = new StorageManager(catalog, allocator, dataDir, MiniDbConfig.load(confDir));
        storage.loadAll();
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        MetadataExecutor metadata = new MetadataExecutor(catalog, allocator);

        boss = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup();
        // 固定大小查询池:cached 池在并发查询下线程无上限,高连接数 × 并发查询会线程爆炸;
        // 单机 OLTP 固定池更稳——大小取配置 server.query-threads(0=自动=可用核数),
        // 超额查询在队列排队,线程数封顶。
        int queryThreads = storage.config().serverQueryThreads();
        queryPool = Executors.newFixedThreadPool(
                queryThreads > 0 ? queryThreads : defaultQueryThreads());
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MessageDecoder());
                        ch.pipeline().addLast(new MessageEncoder());
                        ch.pipeline().addLast(new SessionHandler(executor, metadata, queryPool,
                                storage.transactionManager()));
                    }
                });
        channel = bootstrap.bind(port).sync().channel();
        LOG.info("MiniDB server bound to port {}", port);
    }

    public int port() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    /** 查询线程池(测试用:验证固定大小与封顶行为)。 */
    ExecutorService queryPool() {
        return queryPool;
    }

    private static int defaultQueryThreads() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    /** 启动入口(发行脚本与 mvn exec 共用):--port 覆盖 conf/config.yaml 的 server.port。 */
    public static void main(String[] args) throws Exception {
        int port = -1;
        Path dataDir = Path.of("data");
        Path confDir = Path.of("conf");
        Path pidFile = null;
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i])) {
                port = Integer.parseInt(args[++i]);
            } else if ("--data".equals(args[i])) {
                dataDir = Path.of(args[++i]);
            } else if ("--conf".equals(args[i])) {
                confDir = Path.of(args[++i]);
            } else if ("--pid-file".equals(args[i])) {
                pidFile = Path.of(args[++i]);
            }
        }
        MiniDbConfig config = MiniDbConfig.load(confDir);
        if (port < 0) {
            port = config.serverPort();
        }
        LOG.info("MiniDB starting on port {} with data dir {}, conf dir {}", port, dataDir, confDir);
        MiniDbServer server = new MiniDbServer();
        server.start(port, dataDir, confDir);
        LOG.info("MiniDB listening on port {}", server.port());
        Path pidFileFinal = pidFile;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("MiniDB shutting down");
            server.close();
            if (pidFileFinal != null) {
                try {
                    Files.deleteIfExists(pidFileFinal);
                } catch (IOException e) {
                    LOG.warn("failed to delete pid file: {}", pidFileFinal, e);
                }
            }
        }));
        if (pidFile != null) {
            try {
                Files.writeString(pidFile, String.valueOf(ProcessHandle.current().pid()));
            } catch (IOException e) {
                throw new UncheckedIOException("failed to write pid file: " + pidFile, e);
            }
        }
        Thread.currentThread().join();
    }

    @Override
    public void close() {
        LOG.info("MiniDB server closed");
        if (channel != null) {
            channel.close();
        }
        if (boss != null) {
            boss.shutdownGracefully();
        }
        if (workers != null) {
            workers.shutdownGracefully();
        }
        if (queryPool != null) {
            queryPool.shutdown();
        }
        if (storage != null) {
            storage.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }
}
