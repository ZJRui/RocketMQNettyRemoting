package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.common.RemotingHelper;
import com.sachin.netty.remoting.common.RemotingUtil;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    private static final Logger log = LoggerFactory.getLogger(NettyEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) throws Exception {
        try {
            //encoder之后 数据 包含两部分 header和body
            //header中第一部分是 4字节Int，表明整个数据的长度
            //heawder 中的第二部分是4字节int ，其中第一个字节表示序列化的类型SerializeType， 第2-4字节表示header的长度
            //第三部分是header 数据的具体内容

            ByteBuffer header = msg.encodeHeader();
            out.writeBytes(header);
            byte[] body = msg.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("encode exception ," + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (msg != null) {
                log.error(msg.toString());

            }
            RemotingUtil.closeChannel(ctx.channel());
        }


    }
}
