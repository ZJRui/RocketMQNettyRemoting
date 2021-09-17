package com.sachin.netty.remoting.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class SemaphoreReleaseOnlyOnce {
    private final AtomicBoolean released = new AtomicBoolean(false);
    private final Semaphore semaphore;

    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;

    }

    public void release() {
        if (this.semaphore != null) {
            //确保信号量只被释放一次。 一个ResponseFuture 持有一个SemaphoreReleaseOnlyOnce，多次调用ResponseFuture的release方法
            //会导致多次执行SemaphoreReleaseOnlyOnce的release方法，但是我们需要确保ResponseFuture只会释放一个信号量
            //因此这里会使用AtomicBoolean来记录是否 释放过了信号量
            if (this.released.compareAndSet(false, true)) {
                this.semaphore.release();
            }
        }
    }

    public Semaphore getSemaphore() {
        return this.semaphore;
    }
}
