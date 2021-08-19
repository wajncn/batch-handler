package com.wangjin.handler;

import com.wangjin.domain.BatchDomain;

/**
 * @param <T> 处理的内容
 * @param <R> 返回的结果
 */
@FunctionalInterface
public interface BatchHandler<T extends BatchDomain, R> {

    /**
     * 成功回调函数
     *
     * @param t 处理的内容
     * @param r 返回的结果
     */
    default void success(T t, R r) {
    }

    /**
     * @param t 处理的内容
     * @param e 返回的结果
     */
    default void error(T t, Exception e) {
    }

    R handler(T t);
}
