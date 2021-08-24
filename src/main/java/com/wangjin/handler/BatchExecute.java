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
 */
@Slf4j
public class BatchExecute<T> implements IBatchExecute<T> {

    // 每次处理多少条数据
    private final int nThreads;
    private final BatchHandler<T> execute;
    private final ExecutorService executorService;
    private boolean detailLog = false;


    /**
     * 一次性处理一批集合数据
     *
     * @param execute  处理器
     * @param nThreads 一次处理多少批数据
     */
    public BatchExecute(BatchListHandler<T> execute, int nThreads) {
        this.execute = execute;
        this.nThreads = nThreads;
        executorService = Executors.newFixedThreadPool(nThreads);
    }

    /**
     * 一次性处理一批单个数据
     *
     * @param execute  处理器
     * @param nThreads 一次处理多少批数据
     */
    public BatchExecute(BatchSingleHandler<T> execute, int nThreads) {
        this.execute = execute;
        this.nThreads = nThreads;
        executorService = Executors.newFixedThreadPool(nThreads);
    }


    /**
     * 设置详细日期信息
     *
     * @param detailLog 设置详细日期信息
     * @return BatchExecute<T>
     */
    public BatchExecute<T> setDetailLog(boolean detailLog) {
        this.detailLog = detailLog;
        return this;
    }


    @Override
    public void execute(List<T> domains) {
        if (this.execute instanceof BatchListHandler) {
            batchListHandler(domains, (BatchListHandler<T>) this.execute);
            return;
        }
        if (this.execute instanceof BatchSingleHandler) {
            batchSingleHandler(domains, (BatchSingleHandler<T>) this.execute);
            return;
        }
        throw new IllegalArgumentException("暂不支持");
    }


    public void batchListHandler(final List<T> domains, final BatchListHandler<T> execute) {
        final List<List<T>> partition = partition(domains, this.nThreads);
        final long millis = System.currentTimeMillis();
        log.info("数据共{}条 一批处理{}个  共分为{}批", domains.size(), this.nThreads * this.nThreads, (int) Math.ceil(partition.size() / (double) this.nThreads));

        List<List<List<T>>> partition_list = partition(partition, this.nThreads);

        int i = partition_list.size();
        for (List<List<T>> lists : partition_list) {
            final int finalI = i;
            log.info("还需处理{}批数据", i--);
            CountDownLatch countDownLatch = new CountDownLatch(lists.size());
            for (List<T> list : lists) {
                executorService.execute(() -> {
                    try {
                        if (detailLog) {
                            log.info("开始处理数据 {}", list);
                        }
                        execute.handler(list);
                    } catch (Exception e) {
                        log.error("处理第{}批数据异常,异常数据:{}", finalI, list);
                        throw e;
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


    private void batchSingleHandler(final List<T> domains, final BatchSingleHandler<T> execute) {
        final List<List<T>> partition = partition(domains, this.nThreads);
        final long millis = System.currentTimeMillis();
        log.info("数据共{}条 一批处理{}个  共分为{}批", domains.size(), this.nThreads, partition.size());
        int i = partition.size();
        for (List<T> ts : partition) {
            final int finalI = i;
            log.info("还需处理{}批数据", i--);
            CountDownLatch countDownLatch = new CountDownLatch(ts.size());
            for (T t : ts) {
                executorService.execute(() -> {
                    try {
                        if (detailLog) {
                            log.info("开始处理数据 {}", t);
                        }
                        execute.handler(t);
                    } catch (Exception e) {
                        log.error("处理第{}批数据异常,异常数据:{}", finalI, t);
                        throw e;
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


//    ========================================================================================
//    ========================================================================================
//    ========================================================================================

    private interface BatchHandler<T> {

    }


    /**
     * 批处理接口
     *
     * @param <T>
     */
    @FunctionalInterface
    public interface BatchListHandler<T> extends BatchHandler<T> {
        void handler(List<T> ts);
    }

    /**
     * 单个对象处理接口
     *
     * @param <T>
     */
    @FunctionalInterface
    public interface BatchSingleHandler<T> extends BatchHandler<T> {
        void handler(T t);
    }


    /**
     * 将List分割成若干个等分
     *
     * @param list 需要分割的List
     * @param size 需要分割的数量
     * @param <T>  泛型
     * @return 分割的List对象
     */
    public static <T> List<List<T>> partition(final List<T> list, final int size) {
        final int t = list.size() % size == 0 ? list.size() / size : list.size() / size + 1;
        return IntStream.range(0, t).mapToObj(i -> ListUtil.page(i, size, list)).collect(Collectors.toCollection(() -> new ArrayList<>(size)));
    }
}

