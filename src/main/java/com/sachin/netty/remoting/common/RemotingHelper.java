package com.sachin.netty.remoting.common;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class RemotingHelper {
    public static final String ROCKETMQ_REMOTING = "RocketmqRemoting";
    public static final String DEFAULT_CHARSET = "UTF-8";

    public static String exceptionSimpleDesc(final Throwable throwable) {
        StringBuffer sb = new StringBuffer();
        if (throwable != null) {
            sb.append(throwable.toString());
            StackTraceElement[] stackTrace = throwable.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement element = stackTrace[0];
                sb.append(", ");
                sb.append(element.toString());
            }
        }
        return sb.toString();
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        SocketAddress socketAddress = channel.remoteAddress();
        final String addr = socketAddress != null ? socketAddress.toString() : "";
        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }
            return addr;
        }

        return "";
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress) {

        if (socketAddress != null) {
            final String addr = socketAddress.toString();
            if (addr.length() > 0) {
                return addr.substring(1);
            }

        }
        return "";
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");//localhost:8080
        InetSocketAddress inetSocketAddress = new InetSocketAddress(s[0], Integer.parseInt(s[1]));//域名，端口
        return inetSocketAddress;
    }
}
