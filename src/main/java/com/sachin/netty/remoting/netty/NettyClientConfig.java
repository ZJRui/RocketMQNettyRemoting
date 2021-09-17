package com.sachin.netty.remoting.netty;

import lombok.Data;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
@Data
public class NettyClientConfig {

    private boolean useTLS;

    //工作线程的数量
    private int clientWorkerThreads=4;
    private int clientCallbackExecutorThreads=Runtime.getRuntime().availableProcessors();
    //65535
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;
    private int connectTimeoutMillis = 3000;

    //在指定的事件间隔内没有读写操作 那么将会触发IdleStateEvent
    private int clientChannelMaxIdleTimeSeconds = 120;

    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    private boolean clientPooledByteBufAllocatorEnable=false;
    private boolean clientCloseSocketIfTimeout = false;

}
