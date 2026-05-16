package com.streamhub.broker.netty;

import com.streamhub.broker.BrokerEngine;
import com.streamhub.protocol.ProtocolCodec;
import com.streamhub.protocol.Request;
import com.streamhub.protocol.Response;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class StreamHubChannelHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(StreamHubChannelHandler.class);
    private static final AtomicInteger CONNECTION_SEQ = new AtomicInteger();
    private static final ConcurrentHashMap<String, String> CONNECTION_IDS = new ConcurrentHashMap<>();

    private final BrokerEngine engine;
    private final String connectionId;
    private ByteBuf accumulator = Unpooled.buffer();

    public StreamHubChannelHandler(BrokerEngine engine, io.netty.channel.Channel channel) {
        this.engine = engine;
        this.connectionId = "conn-" + CONNECTION_SEQ.incrementAndGet();
        CONNECTION_IDS.put(channel.id().asShortText(), connectionId);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        accumulator.writeBytes(msg);
        processFrames(ctx);
    }

    private void processFrames(ChannelHandlerContext ctx) {
        while (accumulator.readableBytes() >= 4) {
            accumulator.markReaderIndex();
            int length = accumulator.readInt();
            if (accumulator.readableBytes() < length) {
                accumulator.resetReaderIndex();
                return;
            }
            byte[] frame = new byte[4 + length];
            accumulator.resetReaderIndex();
            accumulator.readBytes(frame);
            try {
                Request request = ProtocolCodec.decodeRequest(frame);
                Response response = engine.handle(request, connectionId);
                byte[] encoded = ProtocolCodec.encode(response);
                ctx.writeAndFlush(Unpooled.wrappedBuffer(encoded));
            } catch (IOException e) {
                log.error("Decode error", e);
                ctx.close();
                return;
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Channel error", cause);
        ctx.close();
    }

}
