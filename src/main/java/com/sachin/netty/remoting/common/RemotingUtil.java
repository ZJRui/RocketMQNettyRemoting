package com.sachin.netty.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class RemotingUtil {
    public static final String OS_NAME = System.getProperty("os.name");
    private static final Logger log = LoggerFactory.getLogger(RemotingUtil.class);
    private static boolean isLinuxPlatform = false;
    private static boolean isWindowsPlatform = false;

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }

    }

    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;

    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

    public static Selector openSelector() throws IOException {
        Selector result = null;
        if (isLinuxPlatform) {
            try {

                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    final Method method = providerClazz.getMethod("provider");
                    if (method != null) {
                        final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                        if (selectorProvider != null) {
                            result = selectorProvider.openSelector();
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Open ePoll Selector for linxu flatform exception ", e);
            }
        }
        if (result == null) {
            result = Selector.open();
        }
        return result;

    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                log.info("closeChannel :close the connection to remote address[{}] result:{}", addrRemote, future.isSuccess());
            }
        });

    }

}
