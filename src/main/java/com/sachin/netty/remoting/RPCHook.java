package com.sachin.netty.remoting;

import com.sachin.netty.remoting.protocol.RemotingCommand;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public interface RPCHook {

    void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    void doAfterResponse(final String remoteAddr, final RemotingCommand request, final RemotingCommand response);
}
