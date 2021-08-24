package com.test;

import com.wangjin.handler.BatchExecute;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

public class Demo {

    @Data
    public static class Adomain {
        private String a;
    }

    public static void main(String[] args) {
        ArrayList<Adomain> adomains = finAll();

        batchList(adomains);
//        batchSingle(adomains);
    }

    private static void batchList(ArrayList<Adomain> adomains) {
        BatchExecute.BatchListHandler<Adomain> handler = new BatchExecute.BatchListHandler<Adomain>() {
            @Override
            public void handler(List<Adomain> t) {
                try {
                    System.out.println("BatchListHandler:"+t.size());
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        BatchExecute<Adomain> execute = new BatchExecute<Adomain>(handler, 5);
        execute.setDetailLog(true).execute(adomains);
    }


    private static void batchSingle(ArrayList<Adomain> adomains) {
        BatchExecute.BatchSingleHandler<Adomain> handler = new BatchExecute.BatchSingleHandler<Adomain>() {
            @Override
            public void handler(Adomain adomain) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        };
        BatchExecute<Adomain> execute = new BatchExecute<Adomain>(handler, 200);
        execute.setDetailLog(false).execute(adomains);
        //[main] 批处理完成 数据共[10000]条  共耗时millis:[6186]
    }

    private static ArrayList<Adomain> finAll() {
        ArrayList<Adomain> adomains = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Adomain adomain = new Adomain();
            adomain.setA("" + i);
            adomains.add(adomain);
        }
        return adomains;
    }
}
