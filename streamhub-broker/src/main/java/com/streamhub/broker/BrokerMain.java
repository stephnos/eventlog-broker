package com.streamhub.broker;

import com.streamhub.broker.health.HealthServer;
import com.streamhub.broker.netty.BrokerServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerMain {
    private static final Logger log = LoggerFactory.getLogger(BrokerMain.class);

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.defaults();
        BrokerEngine engine = new BrokerEngine(config);
        HealthServer health = new HealthServer(config.healthPort(), engine);
        BrokerServer server = new BrokerServer(config, engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down broker");
            server.close();
            health.close();
            engine.close();
        }));

        server.start();
        log.info("Health on http://localhost:{}/health", config.healthPort());
        server.channel().closeFuture().sync();
    }
}
