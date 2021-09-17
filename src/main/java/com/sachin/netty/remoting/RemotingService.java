package com.sachin.netty.remoting;

/**
 * @Author Sachin
 * @Date 2021/3/26
 **/
public interface RemotingService {

    void start();

    void shutDown();

    void registerRPCHook(RPCHook rpcHook);
}
