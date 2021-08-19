package com.wangjin.handler;

import com.google.common.collect.Lists;
import com.wangjin.domain.BatchDomain;

import java.util.List;

public class BatchExecute<T extends BatchDomain, R> {

    // 每次处理多少条数据
    private final int handler_num;
    private final BatchHandler<T, R> execute;

    public BatchExecute(BatchHandler<T, R> execute, int handler_num) {
        this.execute = execute;
        this.handler_num = handler_num;
    }


    public void execute(List<T> domains) {
        List<List<T>> partition = Lists.partition(domains, handler_num);
        System.out.printf("数据共%d条 共分为%d批\n", domains.size(), partition.size());
        int i = 1;
        for (List<T> ts : partition) {
            System.out.printf("开始处理第%d批数据\n",i++);
            for (T t : ts) {
                try {
                    execute.success(t, execute.handler(t));
                } catch (Exception e) {
                    execute.error(t, e);
                }
            }
        }
    }

}
