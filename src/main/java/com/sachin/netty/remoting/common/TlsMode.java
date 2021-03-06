package com.sachin.netty.remoting.common;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
public enum TlsMode {
    DISABLED("disabled"),PERMISSIVE("permissive"),ENFORCING("enforcing");
    private String name;

    TlsMode(String name) {
        this.name = name;
    }

    public static TlsMode parse(String mode) {
        for (TlsMode tlsMode : TlsMode.values()) {
            if (tlsMode.name.equals(mode)) {
                return tlsMode;
            }
        }

        return PERMISSIVE;
    }

    public String getName() {
        return name;
    }
}
