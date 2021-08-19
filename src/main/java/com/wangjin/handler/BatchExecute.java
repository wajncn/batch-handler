package com.wangjin.handler;

import com.wangjin.domain.BatchDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @param <T> 处理的内容
 * @param <R> 返回的结果
 */
public class BatchExecute<T extends BatchDomain, R> {

    // 每次处理多少条数据
    private final int nThreads;
    private final BatchHandler<T, R> execute;
    private final ExecutorService executorService;
    private boolean debug = false;

    public BatchExecute(BatchHandler<T, R> execute, int nThreads) {
        this.execute = execute;
        this.nThreads = nThreads;
        executorService = Executors.newFixedThreadPool(nThreads);
    }

    public BatchExecute<T, R> setDebug(boolean debug) {
        this.debug = debug;
        return this;
    }

    public void execute(List<T> domains) {
        List<List<T>> partition = partition(domains, this.nThreads);
        System.out.printf("[%s] 数据共%d条 共分为%d批\n", Thread.currentThread().getName(), domains.size(), partition.size());
        int i = 1;
        for (List<T> ts : partition) {
            System.out.printf("[%s] 开始处理第%d批数据\n", Thread.currentThread().getName(), i++);
            CountDownLatch countDownLatch = new CountDownLatch(ts.size());
            for (T t : ts) {
                executorService.execute(() -> {
                    try {
                        if (debug) {
                            System.out.printf("[%s] 开始处理数据 [%s] \n ", Thread.currentThread().getName(), t);
                        }
                        this.execute.success(t, this.execute.handler(t));
                    } catch (Exception e) {
                        this.execute.error(t, e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public List<List<T>> partition(List<T> source, int n) {
        if(n > source.size()) n = source.size();
        List<List<T>> result = new ArrayList<List<T>>(n);
        int remainder = source.size() % n;  //(先计算出余数)
        int number = source.size() / n;  //然后是商
        int offset = 0;//偏移量
        for (int i = 0; i < n; i++) {
            List<T> value = null;
            if (remainder > 0) {
                value = source.subList(i * number + offset, (i + 1) * number + offset + 1);
                remainder--;
                offset++;
            } else {
                value = source.subList(i * number + offset, (i + 1) * number + offset);
            }
            result.add(value);
        }
        return result;
    }

}
