package com.sachin.netty.remoting.common;

import lombok.Data;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
@Data
public class Pair<T1,T2> {
    private T1 object1;
    private T2 object2;

    public Pair(T1 object1, T2 object2) {
        this.object1 = object1;
        this.object2 = object2;
    }
}
