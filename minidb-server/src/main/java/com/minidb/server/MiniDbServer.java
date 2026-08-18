package com.minidb.server;

import com.minidb.protocol.MessageDecoder;
import com.minidb.protocol.MessageEncoder;
import com.minidb.server.catalog.MiniDbCatalog;
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
import java.net.InetSocketAddress;
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
        allocator = new RootAllocator();
        storage = new StorageManager(catalog, allocator, dataDir);
        storage.loadAll();
        StatsManager stats = new StatsManager(storage);
        QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
        MetadataExecutor metadata = new MetadataExecutor(catalog, allocator);

        boss = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup();
        queryPool = Executors.newCachedThreadPool();
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MessageDecoder());
                        ch.pipeline().addLast(new MessageEncoder());
                        ch.pipeline().addLast(new SessionHandler(executor, metadata, queryPool));
                    }
                });
        channel = bootstrap.bind(port).sync().channel();
        LOG.info("MiniDB server bound to port {}", port);
    }

    public int port() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
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
