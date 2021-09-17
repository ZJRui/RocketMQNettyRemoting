package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import java.nio.channels.Channel;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public interface NettyRequestProcessor {
    RemotingCommand processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand request);

    boolean rejectRequest();

}
