package com.sachin.netty.remoting;

import io.netty.channel.Channel;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public interface ChannelEventListener {

    void onChannelConnect(final String remoteAddr, final Channel channel);

    void onChannelClose(final String remoteAddr, final Channel channel);

    void onChannelException(final String remoteAddr, final Channel channel);

    void onChannelIdle(final String remoteAddr, final Channel channel);
}
