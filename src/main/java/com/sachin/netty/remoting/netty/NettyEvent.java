package com.sachin.netty.remoting.netty;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
@Data
@AllArgsConstructor
public class NettyEvent {

    private final NettyEventType type;
    private final String remoteAddr;
    private final Channel channel;


}
