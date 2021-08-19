package com.wangjin.handler;

import com.wangjin.domain.BatchDomain;

@FunctionalInterface
public interface BatchHandler<T extends BatchDomain, R> {

    default void success(T t, R r) {
    }

    default void error(T t, Exception e) {
    }

    R handler(T t);
}
