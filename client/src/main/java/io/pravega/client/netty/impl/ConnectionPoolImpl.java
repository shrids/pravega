/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.client.netty.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.pravega.client.ClientConfig;
import io.pravega.client.Session;
import io.pravega.common.Exceptions;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionPoolImpl implements ConnectionPool {

    private final ClientConfig clientConfig;
    private final EventLoopGroup group;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Getter(AccessLevel.PACKAGE)
    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    @GuardedBy("$LOCK")
    private final List<Connection> connectionList = new ArrayList<>();
    private final Collector<Connection, ConnectionSummaryStats, ConnectionSummaryStats> collectorStats =
            Collector.of(ConnectionSummaryStats::new, ConnectionSummaryStats::accept, ConnectionSummaryStats::combine,
                         Collector.Characteristics.IDENTITY_FINISH);

    public ConnectionPoolImpl(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        // EventLoopGroup objects are expensive, do not create a new one for every connection.
        this.group = getEventLoopGroup();
    }

    @Override
    @Synchronized
    public CompletableFuture<ClientConnection> getClientConnection(Session session, PravegaNodeUri location, ReplyProcessor rp) {
        Preconditions.checkNotNull(session, "Session");
        Preconditions.checkNotNull(location, "Location");
        Preconditions.checkNotNull(rp, "ReplyProcessor");
        Exceptions.checkNotClosed(closed.get(), this);

        // Fetch the Connection related stats.
        ConnectionSummaryStats stats = connectionList.parallelStream().collect(collectorStats);
        // Choose the connection with the least number of sessions.
        Optional<Connection> suggestedConnection = stats.getConnectionWithMinimumSession(location);

        final Connection connection;
        if (suggestedConnection.isPresent() && stats.getConnectionCount(location) == clientConfig.getMaxConnectionsPerSegmentStore()) {
            // reuse the connection.
            Connection oldConnection = suggestedConnection.get();
            connectionList.remove(oldConnection);
            connection = new Connection(oldConnection.getUri(), oldConnection.getSessionHandler(), oldConnection.getWriterCount(), oldConnection.getReaderCount(),
                                        oldConnection.getSessionCount() + 1);
        } else {
            // create a new connection.
            CompletableFuture<SessionHandler> sessionHandlerFuture = establishConnection(location);
            connection = new Connection(location, sessionHandlerFuture, 0, 0, 1);
        }
        connectionList.add(connection);
        return connection.getSessionHandler().thenApply(sessionHandler -> sessionHandler.createSession(session, rp));
    }

    /**
     * Establish a new connection to the Pravega Node.
     * @param location The Pravega Node Uri
     * @return A future, which completes once the connection has been established, returning a SessionHandler that can be used to create
     * sessions on the connection.
     */
    private CompletableFuture<SessionHandler> establishConnection(PravegaNodeUri location) {
        final AppendBatchSizeTracker batchSizeTracker = new AppendBatchSizeTrackerImpl();
        final SessionHandler handler = new SessionHandler(location.getEndpoint(), batchSizeTracker);
        final Bootstrap b = getNettyBootStrap().handler(getChannelInitializer(location, batchSizeTracker, handler));

        // Initiate Connection.
        final CompletableFuture<SessionHandler> connectionComplete = new CompletableFuture<>();
        try {
            b.connect(location.getEndpoint(), location.getPort()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        //since ChannelFuture is complete future.channel() is not a blocking call.
                        Channel ch = future.channel();
                        log.debug("Connect operation completed for channel:{}, local address:{}, remote address:{}",
                                  ch.id(), ch.localAddress(), ch.remoteAddress());
                        channelGroup.add(ch); // Once a channel is closed the channel group implementation removes it.
                        connectionComplete.complete(handler);
                    } else {
                        connectionComplete.completeExceptionally(new ConnectionFailedException(future.cause()));
                    }
                }
            });
        } catch (Exception e) {
            connectionComplete.completeExceptionally(new ConnectionFailedException(e));
        }

        final CompletableFuture<Void> channelRegisteredFuture = new CompletableFuture<>(); //to track channel registration.
        handler.completeWhenRegistered(channelRegisteredFuture);

        return connectionComplete.thenCombine(channelRegisteredFuture, (sessionHandler, v) -> sessionHandler);
    }

    /**
     * Create {@link Bootstrap}.
     */
    private Bootstrap getNettyBootStrap() {
        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true);
        return b;
    }

    /**
     * Create a Channel Initializer which is to to setup {@link ChannelPipeline}.
     */
    private ChannelInitializer<SocketChannel> getChannelInitializer(final PravegaNodeUri location,
                                                                    final AppendBatchSizeTracker batchSizeTracker,
                                                                    final SessionHandler handler) {
        final SslContext sslCtx = getSslContext();

        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    SslHandler sslHandler = sslCtx.newHandler(ch.alloc(), location.getEndpoint(), location.getPort());

                    if (clientConfig.isValidateHostName()) {
                        SSLEngine sslEngine = sslHandler.engine();
                        SSLParameters sslParameters = sslEngine.getSSLParameters();
                        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                        sslEngine.setSSLParameters(sslParameters);
                    }
                    p.addLast(sslHandler);
                }
                p.addLast(
                        new LoggingHandler(LogLevel.TRACE),
                        new ExceptionLoggingHandler(location.getEndpoint()),
                        new CommandEncoder(batchSizeTracker),
                        new LengthFieldBasedFrameDecoder(WireCommands.MAX_WIRECOMMAND_SIZE, 4, 4),
                        new CommandDecoder(),
                        handler);
            }
        };
    }

    /**
     * Obtain {@link SslContext} based on {@link ClientConfig}.
     */
    private SslContext getSslContext() {
        final SslContext sslCtx;
        if (clientConfig.isEnableTls()) {
            try {
                SslContextBuilder clientSslCtxBuilder = SslContextBuilder.forClient();
                if (Strings.isNullOrEmpty(clientConfig.getTrustStore())) {
                    clientSslCtxBuilder = clientSslCtxBuilder.trustManager(FingerprintTrustManagerFactory
                                                                                   .getInstance(FingerprintTrustManagerFactory.getDefaultAlgorithm()));
                    log.debug("SslContextBuilder was set to an instance of {}", FingerprintTrustManagerFactory.class);
                } else {
                    clientSslCtxBuilder = SslContextBuilder.forClient()
                                                           .trustManager(new File(clientConfig.getTrustStore()));
                }
                sslCtx = clientSslCtxBuilder.build();
            } catch (SSLException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        } else {
            sslCtx = null;
        }
        return sslCtx;
    }

    private EventLoopGroup getEventLoopGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup();
        } else {
            log.warn("Epoll not available. Falling back on NIO.");
            return new NioEventLoopGroup();
        }
    }

    @Override
    public void close() {
        log.info("Shutting down connection pool");
        if (closed.compareAndSet(false, true)) {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }

    }
}