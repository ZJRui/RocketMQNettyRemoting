package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.ChannelEventListener;
import com.sachin.netty.remoting.InvokeCallback;
import com.sachin.netty.remoting.RPCHook;
import com.sachin.netty.remoting.common.Pair;
import com.sachin.netty.remoting.common.RemotingHelper;
import com.sachin.netty.remoting.common.SemaphoreReleaseOnlyOnce;
import com.sachin.netty.remoting.common.ServiceThread;
import com.sachin.netty.remoting.exception.RemotingSendRequestException;
import com.sachin.netty.remoting.exception.RemotingTimeoutException;
import com.sachin.netty.remoting.exception.RemotingTooMuchRequestException;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import com.sachin.netty.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public abstract class NettyRemotingAbstract {


    protected final Semaphore semaphoreOneway;
    protected final Semaphore semaphoreAsync;

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingAbstract.class);
    //执行器，处理事件交给Listener
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    //Integer (requestCode)-》
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>();
    //key:requestId
    protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

    Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;


    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway);
        this.semaphoreAsync = new Semaphore(permitsAsync);
    }


    public abstract ChannelEventListener getChannelEventListener();

    public abstract RPCHook getRPCHook();

    public abstract ExecutorService getCallbackExecutor();

    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);

    }


    //每隔一秒钟扫描一次responseTable针对过期的Request 丢弃  ResponseFuture
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Map.Entry<Integer, ResponseFuture>> iterator = this.responseTable.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = iterator.next();
            ResponseFuture responseFuture = next.getValue();
            //已经超时了
            if ((responseFuture.getBeginTimestamp() + responseFuture.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                responseFuture.release();
                iterator.remove();
                rfList.add(responseFuture);
                log.warn("remove timout request," + responseFuture
                );
            }
        }
        for (ResponseFuture responseFuture : rfList) {
            try {
                executeInvokeCallback(responseFuture);

            } catch (Throwable throwable) {
                log.warn("scanResponseTable ,operationComplete exception", throwable);
            }
        }
    }

    public void processRequestCommand(final ChannelHandlerContext channelHandlerContext, final RemotingCommand cmd) {
        //接收到请求，获取可以处理该请求的processor以及该processor的线程池
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = matched == null ? this.defaultRequestProcessor : matched;
        //记录请求的id，该ID需要写入到响应中
        final int opaque = cmd.getOpaque();
        if (pair != null) {
            //针对该请求封装成一个Runnable处理
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                        if (rpcHook != null) {
                            rpcHook.doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(channelHandlerContext.channel()), cmd);
                        }
                        //调用业务组件处理请求，然后将处理的响应结果再发送给对方
                        RemotingCommand response = pair.getObject1().processRequest(channelHandlerContext, cmd);
                        if (rpcHook != null) {
                            rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(channelHandlerContext.channel()), cmd, response);
                        }
                        if (!cmd.isOnewayRPC()) {//非一次性
                            if (response != null) {
                                response.setOpaque(opaque);
                                //处理别人发给我的请求，然后我处理完之后 这个RemotingCommand作为响应返回给对方，此时需要标记这个RemotingCommand是response
                                response.markResponseType();
                                try {
                                    channelHandlerContext.writeAndFlush(response);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    log.error("process request over,but response faild", e);
                                    log.error(cmd.toString());
                                    log.error(response.toString());
                                }
                            } else {
                                //response 为空，没有处理结果，所以不需要给对方返回数据
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("process request exception", e);
                        log.error(cmd.toString());
                        if (!cmd.isOnewayRPC()) {//非一次性
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            channelHandlerContext.writeAndFlush(response);
                        }
                    }
                }
            };

            if (pair.getObject1().rejectRequest()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[rejectrequest] System busy,start flwo control for a while");
                response.setOpaque(opaque);
                //使用channel中装配的编码器 对数据进行编码并发送
                channelHandlerContext.writeAndFlush(response);
                return;
            }
            //将run 交给线程处理

            try {
                final RequestTask requestTask = new RequestTask(run, channelHandlerContext.channel(), cmd);
                pair.getObject2().submit(requestTask);//提交给指定的线程池执行
            } catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(channelHandlerContext.channel())
                            + ", too many requests and system thread pool busy, RejectedExecutionException "
                            + pair.getObject2().toString()
                            + " request code: " + cmd.getCode());
                }
                if (!cmd.isOnewayRPC()) {
                    //如果不是OneWayRPC
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "Overload System busy, start flow control for a while");
                    response.setOpaque(opaque);
                    channelHandlerContext.writeAndFlush(response);
                }
            }

        } else {
            //pair为空，找不到processor
            String erro = "request type " + cmd.getCode() + "not support";
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, erro);
            response.setOpaque(opaque);
            channelHandlerContext.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(channelHandlerContext.channel()) + erro);
        }

    }

    /**
     * \
     * * Process response from remote peer to the previous issued requests.
     *
     * @param channelHandlerContext
     * @param cmd
     */
    public void processResponseCommand(final ChannelHandlerContext channelHandlerContext, final RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();
        ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);
            responseTable.remove(opaque);
            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }

        } else {
            log.warn("receive response,but not matched any request ,", RemotingHelper.parseChannelRemoteAddr(channelHandlerContext.channel()));
            log.warn(cmd.toString());

        }

    }

    private void executeInvokeCallback(ResponseFuture responseFuture) {
        boolean runInThisThreawd = false;
        ExecutorService callbackExecutor = this.getCallbackExecutor();
        if (callbackExecutor != null) {
            try {
                callbackExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Exception e) {
                            e.printStackTrace();
                            log.warn("execute callback in executor exception ,and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                //线程池执行失败，尝试本地线程执行
                runInThisThreawd = true;

                log.warn("execute callback in executor exception ,maybe executor busy", e);

            }

        } else {
            // callbackExecutor为空，表示没有指定在某一个线程池中执行，因此就是在本线程中执行
            runInThisThreawd = true;
        }
        if (runInThisThreawd) {
            try {
                responseFuture.executeInvokeCallback();

            } catch (Throwable throwable) {
                log.warn("executeInvokeCallabck Exception", throwable);
            } finally {
                responseFuture.release();
            }
        }


    }

    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        //网络中的数据经过解码器解码成RemotingCommand
        final RemotingCommand cmd = msg;
        //为什么这个地方要区分 request和response呢？ 原因是 我收到的请求 有两种情况（1） 服务器主动发过来的请求，我处理后需要给出响应结果
        //(2)我主动发出的请求，然后等待别人的回应。  别人给我的数据可能 本身就是一个请求，也可能是我某一个请求的响应
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    //如果是别人对我的请求
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;

                default:
                    break;
            }
        }

    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {

        final int opaque = request.getOpaque();

        try {
            //创建一个请求的响应
            final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis, null, null);
            this.responseTable.put(opaque, responseFuture);
            final SocketAddress socketAddress = channel.remoteAddress();
            //将请求发送出去
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {

                    if (future.isSuccess()) {
                        responseFuture.setSendRequestOk(true);
                        return;
                    } else {
                        //发送失败
                        responseFuture.setSendRequestOk(false);
                    }
                    responseTable.remove(opaque);
                    responseFuture.setCause(future.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + socketAddress + "> failed");
                }
            });
            //阻塞当前线程指定的时间 等待响应
            RemotingCommand remotingResponseCommand = responseFuture.waitResponse(timeoutMillis);
            //没有接收到响应的时候需要判断 是否发送成功
            if (null == remotingResponseCommand) {
                if (responseFuture.isSendRequestOk()) {
                    //发送成功，但是没有响应数据
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(socketAddress), timeoutMillis, responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(socketAddress), responseFuture.getCause());
                }
            }
            return remotingResponseCommand;

        } finally {
            //上面已经拿到了response，因此这里必须要 remove调用。因为ResponseTable只记录暂时尚未收到response响应的Request
            this.responseTable.remove(opaque);

        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingSendRequestException, RemotingTooMuchRequestException, RemotingTimeoutException {
        final int opaque = request.getOpaque();
        //获取信号量通行证，放置异步请求发送过多占用系统内存
        boolean acquire = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquire) {
            //获取成功，tryfinally保证信号量被释放.正常情况下是调用  this.semaphoreAsync.release释放信号量，
            //但是在此处我们 将this.semphoreAsync包装成SemaphoreReleaseOnlyOnce，然后将once 交给ResponseFuture
            //因此最终信号量的释放是通过responseFuture.release() 实现的
            //而且这里为什么没有finally 中调用释放信号量？ semaphoreAsync信号量的本质是保证系统内最多有多少个并发异步请求在发送
            //因此信号量的释放操作应该是在 接收到响应之后才进行释放，什么时候能够接收到响应是未知的，因此这里没有释放信号量的操作
            //我们将responseFuture放置到了responseTable中，然后定期扫描responseTable，当发现其中的某一个ResponseFuture已经
            //被设置了响应，那么此时我们就可以说这个ResponseFuture关联到 request请求 已经接受到了响应，此时就可以释放信号量了
            //这就是为什么我们要将this.semaphoreAsync 封装成Once之后交给ResponseFuture的原因
            //但是有一种情况就是如果在发送请求的过程中出现了异常，那么此时需要释放信号量。因此在catch中我们看到了释放信号量的操作
            //问题： 为什么要将信号量保装成SemaphoreReleaseOnlyonce？因为我们知道ResponseFuture在接收到响应的时候需要及时释放信号量
            //你怎么保证 通过ResponseFuture释放信号量的这个操作 只会被执行一次呢？this.semaphoreAsync 的信号量值默认是65536，如果
            //你通过 一个ResponseFuture多次调用了this.semaphoreAsync.release()方法，那么就表示多次释放了信号量，但是
            //实际一个ResponseFuture只持有一个信号量值，他只需要释放一次就可以了。 在SemaphoreReleaseOnlyOnce类中就存在一个AtomicBoolean
            //这个boolean变量就用来记录 ResponseFuture是否释放过了信号量
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis, invokeCallback, once);
            this.responseTable.put(opaque, responseFuture);
            try {
                //发送请求
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {

                        if (future.isSuccess()) {
                            responseFuture.setSendRequestOk(true);
                            return;
                        } else {
                            //发送失败
                            responseFuture.setSendRequestOk(false);

                        }
                        //针对发送失败的情况下做如下处理：
                        //删除记录的responseFuture
                        responseTable.remove(opaque);
                        responseFuture.putResponse(null);
                        //执行异步回调，因为回调函数可能是在当前线程中执行，也可能是在其他线程池中执行，如果在当前线程执行 需要确保出现
                        //异常的情况下 当前线程能够继续执行，因此这里需要try-catch
                        try {
                            executeInvokeCallback(responseFuture);
                        } catch (Exception e) {
                            log.warn("execute callback in writeAndFlush addListener ,and callback throw", e);
                        }
                        //发送成功的情况下 在上面的第一个if中就return了，发送失败而且没有抛出异常的情况下需要主动释放信号量
                        //因为ResponseFuture已经从responseTable 中remove了，因此后续不会有其他线程调用 ResponseFuture来释放其所获得的信号量了
                        //因此在发送失败的情况下 通过release方法释放一个信号量。
                        responseFuture.release();
                    }
                });

            } catch (Exception e) {
                //发送过程出现了异常，及时释放信号量
                responseFuture.release();
                log.warn("send a request command to channel<" + RemotingHelper.parseChannelRemoteAddr(channel) + "> exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        }else{
            //因为没有获取到信号量 超时 导致无法发送请求
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fase");
            }
            String info = String.format("invokeAsyncImpl tryAcquire semaphore timeout ,%dms ,waiting therad num :%d semaphoreAsyncValue:%d", timeoutMillis, this.semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
            log.warn(info);
            throw new RemotingTimeoutException(info);

        }


    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws
            InterruptedException, RemotingSendRequestException, RemotingTooMuchRequestException, RemotingTimeoutException {

        //发送之前设置属性
        request.markOnewayRPC();
        boolean acquire = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquire) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                //发送请求
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        //立即释放信号量，只管发送不管结果如何
                        once.release();
                        if (!future.isSuccess()) {
                            log.warn("send a request command to chanel<" + channel.remoteAddress() + ">failed");
                        }

                    }

                });
            } catch (Exception e) {
                //发送过程出现了异常
                once.release();
                log.warn("write send a  request command to channel<" + channel.remoteAddress() + "> failed");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);

            }

        }else{
            //获取信号量失败
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke to fast");
            }
            String info = String.format("invoekOnewayImpl tryAcquier semphore timeout %dms ,waiting thread numbs: %d semaphoreAsyncValue :%d ", timeoutMillis, this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits());
            log.warn(info);
            throw new RemotingTimeoutException(info);
        }

    }

    //inner-class
    class NettyEventExecutor extends ServiceThread {

        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent nettyEvent) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(nettyEvent);
            } else {
                log.warn("event queue size[{}]enough ,so drop this event{}", this.eventQueue.size(), nettyEvent.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + "service Started");
            final ChannelEventListener channelEventListener = NettyRemotingAbstract.this.getChannelEventListener();
            while (!this.isStopped()) {
                try {
                    NettyEvent nettyEvent = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (nettyEvent != null && channelEventListener != null) {
                        switch (nettyEvent.getType()) {
                            case IDLE:
                                channelEventListener.onChannelIdle(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                                break;
                            case CLOSE:
                                channelEventListener.onChannelClose(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                                break;
                            case CONNECT:
                                channelEventListener.onChannelConnect(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                                break;
                            case EXCEPTION:
                                channelEventListener.onChannelException(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (InterruptedException e) {
                    //这个中断异常有什么作用，当线程被阻塞在poll， 此时存在其他线程想stop这个线程，那么他需要时何止volatile stop变量
                    //为true，但是仅仅设置了他为true 还不够，因为这个线程此时已经被阻塞在了poll，因此可以调用这个线程 的interrupt
                    //然后导致线程抛出中断异常，然后此时就会执行catch，catch执行完了之后就开始执行while条件。
                    //Thread.interrupt()方法不会中断一个正在运行的线程。也就是说阻塞的情况下系统会抛出中断异常。
                    //如果你没有执行 阻塞方法，而是自己一直在running，那么别人 interrupt了我，我该如何及时响应这个中断呢？那就需要自己主动检查
                    log.warn(this.getServiceName() + " service has exception ", e);
                }

            }
            log.info(this.getServiceName() + "service end");

        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }

}
