package com.sachin.netty.remoting;

import com.sachin.netty.remoting.exception.RemotingConnectException;
import com.sachin.netty.remoting.exception.RemotingSendRequestException;
import com.sachin.netty.remoting.exception.RemotingTimeoutException;
import com.sachin.netty.remoting.exception.RemotingTooMuchRequestException;
import com.sachin.netty.remoting.netty.NettyRequestProcessor;
import com.sachin.netty.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @Author Sachin
 * @Date 2021/3/26
 **/
public interface RemotingClient extends RemotingService {

    void updateNameServerAddressList(final List<String> addrs);

    List<String> getNameServerAddressList();

    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws Exception;

    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMills, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

    void invokeOneWay(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingConnectException;

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executorService);

    void setCallbackExecutor(final ExecutorService callbackExecutor);

    boolean isChannelWritable(final String addr);
}
