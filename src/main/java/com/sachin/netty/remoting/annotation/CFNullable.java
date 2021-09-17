package com.sachin.netty.remoting.annotation;

import java.lang.annotation.*;

/**
 * @Author Sachin
 * @Date 2021/3/28
 **/
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})

public @interface CFNullable {
}
