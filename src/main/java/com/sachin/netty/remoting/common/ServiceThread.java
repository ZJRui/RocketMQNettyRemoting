package com.sachin.netty.remoting.common;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Sachin
 * @Date 2021/3/27
 * 后台线程基类
 **/

@Data
public abstract class ServiceThread implements Runnable {


    private final Logger logger = LoggerFactory.getLogger(ServiceThread.class);

    private volatile boolean stopped = false;

    private volatile boolean hasNotified = false;
    protected final Thread thread;
    private static final long JOIN_TIME = 90 * 1000;

    public ServiceThread() {
        //创建了一个线程，来执行当前对象的run方法
        this.thread = new Thread(this, this.getServiceName());
    }

    public abstract String getServiceName();

    public boolean isStopped() {
        return stopped;
    }


    public void shutdown() {
        this.shutDown(false);
    }

    /**
     * 其他线程调用shutDown方法，来终止 变量thread的线程
     *
     * @param interrupt
     */
    public void shutDown(final boolean interrupt) {
        this.stopped = true;
        logger.info("shutdown thread " + this.getServiceName() + "interrupt " + interrupt);
        //为什么要加锁
        synchronized ((this)) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                //为什么要notify
                this.notify();
            }
        }
        try {
            //为什么要interrupt
            if (interrupt) {
                this.thread.interrupt();
            }
            long beginTime = System.currentTimeMillis();
            //为什么要Join。 阻塞当前线程，等待threa的执行完成， 超过指定的毫秒后，不论threa是否结束，当前线程重新恢复执行
            this.thread.join(JOIN_TIME);
            long eclipseTime = System.currentTimeMillis() - beginTime;
            logger.info("join thread " + this.getServiceName() + " eclipse time(ms)" + eclipseTime + "");

        } catch (InterruptedException e) {
            logger.error("intterrputed", e);
        }
    }

    public void start() {
        this.thread.start();
    }
}
