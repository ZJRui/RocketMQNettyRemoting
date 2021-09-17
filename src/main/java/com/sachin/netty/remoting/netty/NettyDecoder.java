package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.common.RemotingUtil;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @Author Sachin
 * @Date 2021/3/27
 * LengthFileBasedFrameDecoder:自定义长度数据包解码器
 * 传输内容中的LengthField长度字段的值，是指存放在数据包中要传输内容的字节数。
 **/
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    private static Logger log = LoggerFactory.getLogger(NettyDecoder.class);
    private static final int FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        //发送的数据包最大长度、长度字段的偏移量（长度字段位于整个数据包内的字节数珠中的下标值）、长度字段自己占用的字节数，,长度字段的偏移量矫正，丢弃的起始字节数
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;

        //为什么这个地方可以强转？ 因为super的decode返回值虽然声明为Object，但是内部实现的返回值确实ByteBuf

        try {
            //encoder之后 数据 包含两部分 header和body
            //header中第一部分是 4字节Int，表明整个数据的长度
            //heawder 中的第二部分是4字节int ，其中第一个字节表示序列化的类型SerializeType， 第2-4字节表示header的长度
            //第三部分是header 数据的具体内容
            //因为我们在上面的构造器中指定了 长度字段占用的大小为4字节，且长度字段的偏移量为0，因此调用super.decode 之后 得到的数据
            //就是剔除长度之后的数据，这部分数据包含了Hearder的长度、Header具体内容和 Body的具体内容
            frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            ByteBuffer byteBuffer = frame.nioBuffer();
            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (frame != null) {
                frame.release();
            }

        }
        return null;


    }
}
