package com.sachin.netty.remoting.protocol;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
public class RemotingSerializable {
    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static byte[] encode(final Object object) {
        final String json = toJson(object, false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public static String toJson(final Object object, boolean prettyFormat) {
        return JSON.toJSONString(object, prettyFormat);
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {

        final String json = new String(data, CHARSET_UTF8);
        return fromJson(json, classOfT);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    public byte[] encode() {
        final String json = this.toJson(false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public String toJson(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }
}
