package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.InvokeCallback;
import com.sachin.netty.remoting.common.SemaphoreReleaseOnlyOnce;
import com.sachin.netty.remoting.protocol.RemotingCommand;
import lombok.Data;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
@Data
public class ResponseFuture {
    //请求id
    private final int opaque;
    private final long timeoutMillis;
    private final long beginTimestamp = System.currentTimeMillis();

    private volatile RemotingCommand responseCommand;

    private final InvokeCallback invokeCallback;
    //对callback只执行一次,false表示还未执行，true表示已经执行了
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    private final SemaphoreReleaseOnlyOnce once;

    //用来阻塞当前线程等待指定的时间
    private final CountDownLatch countDownLatch = new CountDownLatch(1);


    //消息发送完成之后设置为 true，发送失败设置为false
    private volatile boolean sendRequestOk = true;
    private volatile Throwable cause;

    public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce onlyOnce) {
        this.once = onlyOnce;
        this.invokeCallback = invokeCallback;
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.oprationComplete(this);
            }
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        //这个地方为什么要countDown？
        //因为在发送了请求之后 会调用ResponseFuture的waitResponse方法导致当前线程阻塞等待指定的时间。
        //如果在指定的时间内没有收到响应，那么因为超时的问题会导致当前线程恢复继续执行
        //如果在指定的时间内收到了响应？是怎么收到响应的呢？ channel--》processResponse，在processResponse的过程中根据接收到的数据
        //RemotingCommand中的opaque得到 这个请求id对应的ResponseFuture，然后 将接收到的数据 设置到ResponseFuture，也就是调用了
        //当前的putResponse方法，此时就需要及时唤醒还在处于等待状态的线程
        //另外一个问题：如果发送完请求之后调用了waitResponse方法等待指定的时间，然后超时后线程恢复执行， 再然后其他线程接收到了响应，调用了
        //putResponse，此时执行了countDown，那么此时没有任何需要被唤醒的线程，计数器变为0
        this.countDownLatch.countDown();
    }

    /**
     * 如何让调用者等待多久之后再继续执行，可以使用CountDownLatch
     *
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        //等待指定的时间，等待其他线程调用countDownLatch的countDown方法来唤醒当前线程。
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);

        return this.responseCommand;
    }

}

