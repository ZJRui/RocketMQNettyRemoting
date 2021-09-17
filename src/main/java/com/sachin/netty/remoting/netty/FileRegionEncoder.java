package com.sachin.netty.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
public class FileRegionEncoder extends MessageToByteEncoder<FileRegion> {

    @Override
    protected void encode(ChannelHandlerContext ctx, FileRegion msg, ByteBuf out) throws Exception {

    }
}
