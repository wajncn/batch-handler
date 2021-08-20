package com.wangjin.handler;

import java.util.List;

/**
 * @param <T> 处理的内容
 * @param <R> 返回的结果
 */
@FunctionalInterface
public interface IBatchExecute<T> {

    void execute(List<T> domains);
}
