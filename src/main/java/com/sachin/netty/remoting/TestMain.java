package com.sachin.netty.remoting;

import com.sachin.netty.remoting.protocol.RemotingCommand;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class TestMain {
    public RemotingCommand command = new RemotingCommand();
    public static void main(String[] args) {

        TestMain testMain = new TestMain();
        RemotingCommand remotingCommand = null;
        testMain.testRefer(remotingCommand);
        System.out.println(remotingCommand);

    }

    public void testRefer(RemotingCommand cmd) {
        if (cmd == null) {
            cmd = command;
        }
        System.out.println("cmd:"+cmd);
    }

}
