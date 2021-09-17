package com.sachin.netty.remoting.netty;

import com.sachin.netty.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import lombok.Data;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
@Data
public class RequestTask implements Runnable {

    private final Runnable runnable;
    private final long createTimestamp = System.currentTimeMillis();
    private final Channel channel;
    private final RemotingCommand request;
    private boolean stopRun = false;

    public RequestTask(Runnable runnable, Channel channel, RemotingCommand request) {
        this.runnable = runnable;
        this.channel = channel;
        this.request = request;
    }

    @Override
    public int hashCode() {
        int result = runnable != null ? runnable.hashCode() : 0;
        result = 31 * result + (int) (getCreateTimestamp() ^ (getCreateTimestamp() >>> 32));
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (isStopRun() ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RequestTask)) {
            return false;
        }
        final RequestTask that = (RequestTask) obj;
        if (getCreateTimestamp() != that.getCreateTimestamp()) {
            return false;
        }
        if (isStopRun() != that.isStopRun()) {
            return false;
        }
        if (channel != null ? !channel.equals(that.channel) : that.channel != null) {
            return false;
        }
        //这里很特别，这里没有对Request对象做判断，而是仅仅对Request中的opaque做了对比
        //我们需要保证的是hashCode 相等的时候 equal一定是相等的。但是equal相等的时候 hashCode可以不同
        //因此对 两个持有 相同Request对象的RequestTask是相同的
        return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;
    }

    @Override
    public void run() {

        if (!this.stopRun) {
            this.runnable.run();
        }
    }

    public void returnResponse(int code, String remark) {
        final RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(code, remark);
        remotingCommand.setOpaque(request.getOpaque());
        this.channel.writeAndFlush(remotingCommand);

    }
}
