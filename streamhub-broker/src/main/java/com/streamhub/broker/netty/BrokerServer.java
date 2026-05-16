package com.streamhub.broker.netty;

import com.streamhub.broker.BrokerConfig;
import com.streamhub.broker.BrokerEngine;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

    private final BrokerConfig config;
    private final BrokerEngine engine;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private io.netty.channel.Channel channel;

    public BrokerServer(BrokerConfig config, BrokerEngine engine) {
        this.config = config;
        this.engine = engine;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        var bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new StreamHubChannelHandler(engine, ch));
                    }
                });
        channel = bootstrap.bind(config.port()).sync().channel();
        log.info("Broker listening on port {}", config.port());
    }

    public io.netty.channel.Channel channel() {
        return channel;
    }

    @Override
    public void close() {
        if (channel != null) channel.close();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }
}
