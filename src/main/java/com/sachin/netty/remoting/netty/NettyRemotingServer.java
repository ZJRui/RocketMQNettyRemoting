package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.*;
import com.sachin.netty.remoting.common.Pair;
import com.sachin.netty.remoting.common.RemotingHelper;
import com.sachin.netty.remoting.common.RemotingUtil;
import com.sachin.netty.remoting.common.TlsMode;
import com.sachin.netty.remoting.exception.RemotingSendRequestException;
import com.sachin.netty.remoting.exception.RemotingTimeoutException;
import com.sachin.netty.remoting.exception.RemotingTooMuchRequestException;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final Logger log = LoggerFactory.getLogger(NettyRemotingServer.class);

    private DefaultEventExecutorGroup defaultEventExecutorGroup;


    private NettyServerConfig nettyServerConfig;

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGoupBoss;
    private final EventLoopGroup eventLoopGroupSelector;

    private static final String HandShake_heandler_name = "handShakeHandler";
    private static final String Tls_handler_name = "sslHandler";
    private static final String File_region_encoder_name = "fileRegionEncoder";

    protected SslContext sslContext;
    private final ChannelEventListener channelEventListener;
    private int port = 0;
    ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    private final ExecutorService publicExecutor;
    private RPCHook rpcHook;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(NettyServerConfig config, ChannelEventListener channelEventListener) {

        super(config.getServerOnewaySemaphoreValue(), config.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig=config;
        this.channelEventListener = channelEventListener;
         int publicThreadNums=config.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + threadIndex.incrementAndGet());
            }
        });
        this.eventLoopGoupBoss=new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyBoos_%d", this.threadIndex.incrementAndGet()));
            }
        });
        if (useEpoll()) {
            ThreadFactory threadFactory;
            this.eventLoopGroupSelector=new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEpollSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));


                }
            });
        }else{
            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
            }
        });
        //设置两大线程组，确定了线程模型， 第一个是bossGroup，第二个是workerGroup
        ServerBootstrap childHandler = this.serverBootstrap.group(this.eventLoopGoupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(defaultEventExecutorGroup, HandShake_heandler_name, new HandShakeHandler(TlsSystemConfig.tlsMode))
                                .addLast(defaultEventExecutorGroup, new NettyEncoder(),
                                        new NettyDecoder(), new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds())
                                        , new NettyConnectManageHandler(), new NettyServerHandler());
                    }
                });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress socketAddress = (InetSocketAddress) sync.channel().localAddress();
            this.port = socketAddress.getPort();
        } catch (InterruptedException e) {
            throw new RuntimeException("this serverbootstrap.bind.sync InterruptException", e);
        }
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();

        }
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {

                NettyRemotingServer.this.scanResponseTable();

            } catch (Exception e) {
                log.error("ScannResponseTable exception", e);
            }
        }, 1000 * 3L, 1000L, TimeUnit.MILLISECONDS);


    }

    @Override
    public void shutDown() {
        this.scheduledExecutorService.shutdown();

        this.eventLoopGoupBoss.shutdownGracefully();
        this.eventLoopGroupSelector.shutdownGracefully();
        if (this.nettyEventExecutor != null) {
            this.nettyEventExecutor.shutdown();

        }
        if (this.defaultEventExecutorGroup != null) {
            this.defaultEventExecutorGroup.shutdownGracefully();
        }
        if (this.publicExecutor != null) {
            this.publicExecutor.shutdown();

        }

    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public RPCHook getRPCHook() {
        return this.rpcHook;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }


    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform() && nettyServerConfig.isUseEpollNativeSelector() && Epoll.isAvailable();
    }

    //inner-class
    class HandShakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;
        private static final byte HandShake_magic_code = 0x16;

        public HandShakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            //标记一下读取的位置
            msg.markReaderIndex();
            byte b = msg.getByte(0);
            if (b == HandShake_magic_code) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("clients intentd to establish a ssl connection while this sever is running in ssl disable mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null == sslContext) {
                            ctx.pipeline().addAfter(defaultEventExecutorGroup, HandShake_heandler_name, Tls_handler_name, sslContext.newHandler(ctx.channel().alloc()))
                                    .addAfter(defaultEventExecutorGroup, Tls_handler_name, File_region_encoder_name, new FileRegionEncoder());
                            log.info("Handler prepended to channel pipeline to establish ssl connection");
                        } else {
                            ctx.close();
                            log.error("trying to establish a ssl Connection but sslContext is null");

                        }
                        break;
                    default:
                        log.warn("unknown tls mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("client intend to establish an insecure connection whiel this server is running in ssl enforcing mode");
            }
            msg.resetReaderIndex();
            try {
                //remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("");
            }
            //hand over this message to the next
            ctx.fireChannelRead(msg.retain());


        }
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                                .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
