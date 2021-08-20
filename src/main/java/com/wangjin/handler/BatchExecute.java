package com.wangjin.handler;

import cn.hutool.core.collection.ListUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * @param <T> 处理的内容
 * @param <R> 返回的结果
 */
@Slf4j
public class BatchExecute<T, R> implements IBatchExecute<T> {

    // 每次处理多少条数据
    private final int nThreads;
    private final BatchHandler<T, R> execute;
    private final ExecutorService executorService;
    private boolean debug = false;

    public BatchExecute(BatchListHandler<T, R> execute, int nThreads) {
        this.execute = execute;
        this.nThreads = nThreads;
        executorService = Executors.newFixedThreadPool(nThreads);
    }

    public BatchExecute(BatchSingleHandler<T, R> execute, int nThreads) {
        this.execute = execute;
        this.nThreads = nThreads;
        executorService = Executors.newFixedThreadPool(nThreads);
    }

    public BatchExecute<T, R> setDebug(boolean debug) {
        this.debug = debug;
        return this;
    }


    @Override
    public void execute(List<T> domains) {
        if (this.execute instanceof BatchListHandler) {
            batchListHandler(domains, (BatchListHandler<T, R>) this.execute);
            return;
        }
        if (this.execute instanceof BatchSingleHandler) {
            batchSingleHandler(domains, (BatchSingleHandler<T, R>) this.execute);
            return;
        }
        throw new IllegalArgumentException("暂不支持");
    }


    public void batchListHandler(final List<T> domains, final BatchListHandler<T, R> execute) {
        final List<List<T>> partition = partition(domains, this.nThreads);
        final long millis = System.currentTimeMillis();
        log.info("数据共{}条 一批处理{}个  共分为{}批", domains.size(), this.nThreads * this.nThreads, (int) Math.ceil(partition.size() / (double) this.nThreads));

        List<List<List<T>>> partition_list = partition(partition, this.nThreads);

        int i = partition_list.size();
        for (List<List<T>> lists : partition_list) {
            log.info("还需处理{}批数据", i--);
            CountDownLatch countDownLatch = new CountDownLatch(lists.size());
            for (List<T> list : lists) {
                executorService.execute(() -> {
                    try {
                        if (debug) {
                            log.info("开始处理数据 {}", list);
                        }
                        execute.success(list, execute.handler(list));
                    } catch (Exception e) {
                        execute.error(list, e);
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

        this.shutdown();
        log.info("批处理完成 数据共{}条  共耗时millis:{}", domains.size(), System.currentTimeMillis() - millis);
    }


    private synchronized void shutdown() {
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void batchSingleHandler(final List<T> domains, final BatchSingleHandler<T, R> execute) {
        final List<List<T>> partition = partition(domains, this.nThreads);
        final long millis = System.currentTimeMillis();
        log.info("数据共{}条 一批处理{}个  共分为{}批", domains.size(), this.nThreads, partition.size());
        int i = partition.size();
        for (List<T> ts : partition) {
            log.info("还需处理{}批数据", i--);
            CountDownLatch countDownLatch = new CountDownLatch(ts.size());
            for (T t : ts) {
                executorService.execute(() -> {
                    try {
                        if (debug) {
                            log.info("开始处理数据 {}", t);
                        }
                        execute.success(t, execute.handler(t));
                    } catch (Exception e) {
                        execute.error(t, e);
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

        this.shutdown();
        log.info("批处理完成 数据共{}条  共耗时millis:{}", domains.size(), System.currentTimeMillis() - millis);
    }


//    ========================================================================================
//    ========================================================================================
//    ========================================================================================

    interface BatchHandler<T, R> {

    }


    public interface BatchListHandler<T, R> extends BatchHandler<T, R> {

        R handler(List<T> t);

        /**
         * 成功回调函数
         *
         * @param t 处理的内容
         * @param r 返回的结果
         */
        void success(List<T> t, R r);

        /**
         * @param t 处理的内容
         * @param e 返回的结果
         */
        void error(List<T> t, Exception e);

    }


    public interface BatchSingleHandler<T, R> extends BatchHandler<T, R> {

        R handler(T t);

        /**
         * 成功回调函数
         *
         * @param t 处理的内容
         * @param r 返回的结果
         */
        void success(T t, R r);

        /**
         * @param t 处理的内容
         * @param e 返回的结果
         */
        void error(T t, Exception e);

    }


    public static <T> List<List<T>> partition(final List<T> list, final int size) {
        final int t = list.size() % size == 0 ? list.size() / size : list.size() / size + 1;
        return IntStream.range(0, t).mapToObj(i -> ListUtil.page(i, size, list)).collect(Collectors.toCollection(() -> new ArrayList<>(size)));
    }
}

