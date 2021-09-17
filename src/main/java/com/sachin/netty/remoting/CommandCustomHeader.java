package com.sachin.netty.remoting;

import com.sachin.netty.remoting.exception.RemotingCommandException;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public interface CommandCustomHeader {
    void checkFields() throws RemotingCommandException;
}
