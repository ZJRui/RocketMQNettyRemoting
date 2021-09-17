package com.sachin.netty.remoting;

import com.sachin.netty.remoting.annotation.CFNullable;
import com.sachin.netty.remoting.exception.RemotingCommandException;
import com.sachin.netty.remoting.netty.*;
import com.sachin.netty.remoting.protocol.LanguageCode;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
public class RemotingServerTest {

    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;


    public static RemotingServer createRemotingServer() throws InterruptedException {

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        RemotingServer remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.registerProcessor(0, new NettyRequestProcessor() {

            @Override
            public RemotingCommand processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand request) {

                request.setRemark("Hi," + channelHandlerContext.channel().remoteAddress());

                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newCachedThreadPool());
        remotingServer.start();
        return remotingServer;
    }

    public static RemotingClient createRemotingClient(NettyClientConfig nettyClientConfig) {
        RemotingClient remotingClient = new NettyRemotingClient(nettyClientConfig);
        remotingClient.start();
        return remotingClient;
    }

    public static RemotingClient createRemotingClient() {
        return createRemotingClient(new NettyClientConfig());
    }

    @BeforeClass
    public static void setUp() throws InterruptedException {

       // remotingServer = createRemotingServer();
        remotingClient = createRemotingClient();
    }

    @AfterClass
    public static void destory() {
       // remotingServer.shutDown();
        remotingClient.shutDown();

    }

    @Test
    public void testInvokeSync() throws Exception {

        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000000 * 3);
        assertTrue(response != null);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);

    }

    class RequestHeader implements CommandCustomHeader {
        @CFNullable
        private Integer count;

        @CFNullable
        private String messageTitle;

        @Override
        public void checkFields() throws RemotingCommandException {
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getMessageTitle() {
            return messageTitle;
        }

        public void setMessageTitle(String messageTitle) {
            this.messageTitle = messageTitle;
        }
    }

}
