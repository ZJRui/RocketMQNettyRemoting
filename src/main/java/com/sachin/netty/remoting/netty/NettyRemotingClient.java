package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.ChannelEventListener;
import com.sachin.netty.remoting.InvokeCallback;
import com.sachin.netty.remoting.RPCHook;
import com.sachin.netty.remoting.RemotingClient;
import com.sachin.netty.remoting.common.Pair;
import com.sachin.netty.remoting.common.RemotingHelper;
import com.sachin.netty.remoting.common.RemotingUtil;
import com.sachin.netty.remoting.exception.RemotingConnectException;
import com.sachin.netty.remoting.exception.RemotingSendRequestException;
import com.sachin.netty.remoting.exception.RemotingTimeoutException;
import com.sachin.netty.remoting.exception.RemotingTooMuchRequestException;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final NettyClientConfig nettyClientConfig;

    private Bootstrap bootstrap = new Bootstrap();
    //工作线程池，这个是Netty的线程池
    private EventLoopGroup eventLoopGroupWorker;
    //EventExecutorGroup 注意，EventExecutorGroup和EventLoopGroup是两个不同的概念。
    //通常ChannelPipeline中的每一个ChannelHandler都是通过他的EventLoop(IO线程)来处理传递给他的事件的。
    // 所以不要阻塞这个线程。 但是有时可能需要与那些使用阻塞API的遗留代码进行交互。对于这种情况，channelPipeline有一些接
    // 受一个EventExecutorGroup的add方法。如果一个事件被传递给一个自定义的EventExecutorGroup，他将被包含在这个EventExecutorGroup中的
    // 某个EventExecutor所处理，从而被从该channel本身的EventLoop中移除。对于这种用例，Netty提供了一个叫做DefaultEventExecutorGroup的默认实现
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private final ChannelEventListener channelEventListener;
    private final Lock lockChannelTables = new ReentrantLock();
    //尝试获取锁的超时时间
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    //remoteAddr->channel
    private final ConcurrentHashMap<String, ChannelWrapper> channelTables = new ConcurrentHashMap<>();

    protected SslContext sslContext;


    //private final Timer timer = new Timer("ClientHouseKeepingService", true);
    ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    private final ExecutorService publicExecutor;
    private ExecutorService callbackExecutor;
    private RPCHook rpcHook;

    //nameServer
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    private final Lock lockNamesrvChannel = new ReentrantLock();
    private final AtomicInteger namesrvIndex = new AtomicInteger(0);


    public NettyRemotingClient(NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig, final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;
        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {

                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });
        //todo 忽略sslContext设置
        if (nettyClientConfig.isUseTLS()) {
        }


    }

    private static int initValueIndex() {
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {

                return new Thread(r, "NettyClientWorkThread_" + this.threadIndex.incrementAndGet());
            }
        });

        //channel方法设置通道的IO类型（BIO或者NIO）
        Bootstrap handler = this.bootstrap.group(eventLoopGroupWorker).channel(NioSocketChannel.class)
                //tcp_nodelay:tcp参数表示是否立即发送数据。如果为false表示不理解发送，则会使用Nagle算法将小的碎片数据连接成更大的报文或者
                //数据包来最小化所发送报文的数量
                .option(ChannelOption.TCP_NODELAY, true)

                //《Java网络编程精解》 为true表示TCP实现会监视该链接是否有效。当链接处于空闲状态超过了两个小时，本地的TCP实现会发送一个数据包给远程
                //的socket，如果远程socket没有发回响应,TCP实现就会持续尝试11分钟，直到收到响应为止，如果在12分钟内未收到响应，TCP实现就会自动关闭本地socket，断开连接。
                //默认为false，表示不会监视连接是否有效，不活动的客户端可能会永久存在下去，而不会注意到服务器已经崩溃了
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                //so_rcvBuf so_sndBuf：每个TCP套接字在内核中都有一个发送缓冲区和接收缓冲区，这两个选项就是用来设置TCP连接的两个缓冲区的大小
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
                //装配子通道流水线。有连接到达时会创建一个通道的子通道，并初始化
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        //出站的顺序是从管道的后部向前开始处理的，因此最后添加到尾部的handler先处理
                        if (nettyClientConfig.isUseTLS()) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                        }
                        //DefaultEventExecutorGroup为执行ChannelHandler的线程
                        pipeline.addLast(defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                                new NettyConnectManageHandler(),
                                //NettyClientHandler是一个InBound的handler，因此他只会处理接收到的数据，写出的数据并不经过nettyClientHandler
                                new NettyClientHandler()
                        );


                    }
                });

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                NettyRemotingClient.this.scanResponseTable();
            } catch (Throwable throwable) {
                log.error("scanResponseTable exception ", throwable);
            }
        }, 1000 * 3L, 1000L, TimeUnit.MILLISECONDS);

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }

    }


    @Override
    public void shutDown() {
        try {
            this.scheduledExecutorService.shutdown();

            for (ChannelWrapper channelWrapper : this.channelTables.values()) {
                this.closeChannel(null, channelWrapper.getChannel());
            }
            this.channelTables.clear();
            this.eventLoopGroupWorker.shutdownGracefully();
            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Throwable throwable) {

        }
        if (this.publicExecutor != null) {
            this.publicExecutor.shutdown();
        }

    }


    public void closeChannel(final Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //如果获取到了所
                    boolean remoteItemFromTabel = true;
                    ChannelWrapper prevCW = null;
                    String remoteAddrs = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {

                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null && prev.getChannel() == channel) {
                            prevCW = prev;//找到要被移除的channel
                            remoteAddrs = key;
                            break;
                        }

                        if (prevCW == null) {//表示该channel从channelTable中没有找到，所以下面就不需要移除
                            log.info("eventCloseChannel: the channel [{}] has been removed from the channel table before", remoteAddrs);
                            remoteItemFromTabel = false;//没有找到，设置为false 下面不会进行remove close动作
                        }
                        if (remoteItemFromTabel) {//默认为true表示需要移除
                            this.channelTables.remove(remoteAddrs);
                            log.info("close channel :the channel[{}] was removed from channle table", remoteAddrs);
                            //这里 将closeChannel操作放在了if中，其实也可以放在if外面。参考下文的介绍
                            RemotingUtil.closeChannel(channel);
                        }
                    }
                } catch (Exception e) {
                    log.error("closechannel :close the channel exception", e);
                } finally {
                    //确保锁被释放
                    this.lockChannelTables.unlock();
                }
            } else {

                //获取锁失败，超时
                log.warn("close channel :try to lock channel table ,but timeout ,{}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (Exception e) {
            log.error("clsoeChannel exception", e);
        }
    }

    public void closeChannel(String remoteAddr, final Channel channel) {

        if (null == channel) {
            return;
        }
        final String addrRemote = remoteAddr == null ? RemotingHelper.parseChannelRemoteAddr(channel) : remoteAddr;
        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                //获取锁成功. 为保证锁能够被释放，内部需要使用另一个try finally
                try {
                    // 从table中找到被关闭的Channel
                    boolean removeItemFromTable = true;
                    ChannelWrapper channelWrapper = this.channelTables.get(addrRemote);
                    if (channelWrapper == null) {
                        //未找到channel
                        log.info("closeChannel :the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (channelWrapper.getChannel() != channel) {
                        //根据地址从table中找到的channel和要关闭的channel不是同一个channel，可能table中存放的channel是新建立的channel，
                        //而此处方法接收到的channel是一个 已经断了连接的channel
                        removeItemFromTable = false;
                    }
                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                    }
                    //这个closeChannel为什么没有放置到上面的if中，因为不管 当前channel是否能够在channelTable中找到该channel都需要关闭
                    //（1）如果根据地址从table中找到的channel和当前的channel不匹配，则说明方法入参channel是一个过期的channel需要被关闭
                    //(2)如果从table中找到的channel 等于方法的channel，则也需要关闭。因此无论如何该channel都需要被 关闭

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {

                    log.error("closeChannel error ", e);
                } finally {
                    //确保释放锁
                    this.lockChannelTables.unlock();

                }

            }//if
            else {
                //获取锁失败
                log.warn("NettyConnectMananagerHandlerCloseChannel: try to lock channel table ,but timeout ,{} ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (Throwable throwable) {

            log.error("close channel exception", throwable);
        }
    }


    @Override
    public void updateNameServerAddressList(List<String> addrs) {

    }

    private Channel getAndCreateChannel(final String addr) throws InterruptedException {

        if (null == addr) {
            return getAndCreateNameserverChannel();
        }
        ChannelWrapper channelWrapper = this.channelTables.get(addr);
        if (channelWrapper != null && channelWrapper.isOk()) {
            return channelWrapper.getChannel();
        }
        return this.createChannel(addr);

    }

    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        //获取被随机选中的nameSrv
        String addr = namesrvAddrChoosed.get();
        //如果地址不为空，则表示之前随机选中了某一个nameSrv
        if (addr != null) {

            ChannelWrapper channelWrapper = this.channelTables.get(addr);
            if (channelWrapper != null && channelWrapper.isOk()) {
                return channelWrapper.getChannel();
            }
        }
        final List<String> addrsList = this.namesrvAddrList.get();
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            //获取锁成功
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper channelWrapper = this.channelTables.get(addr);
                    if (channelWrapper != null && channelWrapper.isOk()) {
                        return channelWrapper.getChannel();
                    }
                }
                //为什么要遍历？todo,确保可以找到一个channel
                if (addrsList != null && !addrsList.isEmpty()) {
                    for (int i = 0; i < addrsList.size(); i++) {
                        //生成一个索引
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrsList.size();
                        //从List中随机选择一个地址
                        String newAddr = addrsList.get(index);
                        //设置成被选择的nameSrv
                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new Name server is chosen ,Old:{}, new {},namesrvIndex={}", addr, newAddr, namesrvIndex);
                        // 根据被选中的nameSrv的地址 创建一个channel
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }

                    }
                }

            } catch (Exception e) {

            } finally {
                this.lockNamesrvChannel.unlock();
            }
        }

        return null;
    }

    private Channel createChannel(String addr) throws InterruptedException {

        ChannelWrapper channelWrapper = this.channelTables.get(addr);
        if (channelWrapper != null && channelWrapper.isOk()) {
            channelWrapper.getChannel().close();
            this.channelTables.remove(addr);
        }
        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {

            try {

                boolean createNewConnection;
                channelWrapper = this.channelTables.get(addr);
                if (channelWrapper != null) {
                    if (channelWrapper.isOk()) {//channel isActive
                        channelWrapper.getChannel().close();
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    } else if (channelWrapper.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }
                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    log.info("create channel :begin to connect remote host{} asynchronously", addr);
                    channelWrapper = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, channelWrapper);
                }
            } catch (Exception e) {
                log.error("crate channel exceptino ", e);

            } finally {
                this.lockChannelTables.unlock();

            }
        } else {
            //获取锁失败
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (channelWrapper != null) {
            ChannelFuture channelFuture = channelWrapper.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (channelWrapper.isOk()) {
                    log.info("create channel :connect remote host[{}] success", addr, channelFuture.toString());
                    return channelWrapper.getChannel();
                } else {
                    log.warn("create channel: connect remote host{}failed", channelFuture.toString(), channelFuture.cause());
                }
            } else {
                log.warn("createChannel:connect remote host[{}] timeout {}ms ,{}", addr, this.nettyClientConfig.getConnectTimeoutMillis());
            }

        }
        return null;
    }


    /**
     * 同步发送数据
     *
     * @param addr
     * @param request
     * @param timeoutMillis
     * @return
     * @throws Exception
     */
    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {

            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                if (this.rpcHook != null) {
                    this.rpcHook.doAfterResponse(addr, request, response);
                }
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invoke async send request exception ,so close  the channel [{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                //连接失败的情况下判断是否是需要关闭channel
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {

                    this.closeChannel(addr, channel);
                    log.warn("invokeSync :close socket because of timeout,{} ms,{}", timeoutMillis, addr);
                }
                log.warn("invokeSync:wait response timeout exception ,the channel{}", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMills, InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {

        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {

            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);

                }
                this.invokeAsyncImpl(channel, request, timeoutMills, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync:send request exception ,so close the channel{}", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneWay(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
            RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingConnectException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                this.invokeOnewayImpl(channel, request, timeoutMillis);

            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneWay:send request exception ,so close the channel{}", addr);
                this.closeChannel(addr, channel);
                throw e;
            }

        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }

    }


    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executorService) {
        ExecutorService executorThis = executorService;
        if (executorService == null) {
            executorThis = this.publicExecutor;
        }
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);

    }

    @Override
    public void setCallbackExecutor(ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;

    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper channelWrapper = channelTables.get(addr);
        if (channelWrapper != null && channelWrapper.isOk()) {
            return channelWrapper.isWritable();
        }
        //return false;
        //todo
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }


    @Override
    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

    @Override
    public RPCHook getRPCHook() {
        return this.rpcHook;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.callbackExecutor != null ? this.callbackExecutor : publicExecutor;
    }


    //Inner-Class
    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {


        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {

            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("netty client pipeline :connect {}=>{}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, promise);
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("netty client pipeline :disconnect {}", remoteAddress);
            //问题：这里为什么要自己调用 channel.close 关闭channel，而不是 通过super.disConnect去关闭
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("netty client pipeline :close {}", remoteAddr);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddr, ctx.channel()));
            }
        }

        /**
         * 实现userEventTriggered 作为超时事件的逻辑处理
         * server端：设定IdleStatehandler心跳检测每5秒进行一次读检测，如果五秒内ChannelRead方法未被调用则触发一次userEventTriggered方法
         * client端：设定IUdleStateHandler心跳检测每4秒进行一次写检测，如果四秒内write方法未被调用则触发一次userEventTrigger方法，
         *
         * @param ctx
         * @param evt
         * @throws Exception
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                //All_IDLE:有一段时间没有接收或发送数据。如果检测到有的一段时间内没有发送和接收数据那么就关闭连接
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("netty clinet pipeline :idle exceptino[{}]", remoteAddr);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddr, ctx.channel()));
                    }
                }


            }
            //触发下一个handler的fireUserEventTrigger
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("netty client pipeline: executionCaugh {}", remoteAddr);
            log.warn("netty clinet pipeline: executioCaugh exception", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddr, ctx.channel()));

            }
        }
    }

    class ChannelWrapper {


        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOk() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            //当且仅当I/O线程将立即执行请求的写操作时，返回true。当这个方法返回false时发出的任何写请求都将进入队列，直到I/O线程准备好处理排队的写请求进入翻译页面
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }


    }


}
