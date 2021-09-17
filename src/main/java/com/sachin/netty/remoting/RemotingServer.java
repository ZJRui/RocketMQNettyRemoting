package com.sachin.netty.remoting;

import com.sachin.netty.remoting.common.Pair;
import com.sachin.netty.remoting.exception.RemotingSendRequestException;
import com.sachin.netty.remoting.exception.RemotingTimeoutException;
import com.sachin.netty.remoting.exception.RemotingTooMuchRequestException;
import com.sachin.netty.remoting.netty.NettyRequestProcessor;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
public interface RemotingServer  extends  RemotingService{
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    int localListenPort();

    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
                               final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException;

    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                     final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException;
}
