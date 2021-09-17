package com.sachin.netty.remoting;

import com.sachin.netty.remoting.netty.ResponseFuture;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public interface InvokeCallback {

    void oprationComplete(final ResponseFuture responseFuture);
}
