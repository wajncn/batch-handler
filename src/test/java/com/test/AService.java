package com.test;

import com.wangjin.handler.BatchExecute;
import com.wangjin.handler.BatchHandler;

import java.util.ArrayList;

public class AService {

    public static void main(String[] args) {
        ArrayList<Adomain> adomains = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            Adomain adomain = new Adomain();
            adomain.setA("" + i);
            adomains.add(adomain);
        }
        System.out.println("adomains.size() = " + adomains.size());


        BatchHandler<Adomain, String> handler = new BatchHandler<Adomain, String>() {

            @Override
            public void success(Adomain adomain, String s) {
//                System.out.println("s = " + s + "  adomain:" + adomain);
            }

            @Override
            public void error(Adomain adomain, Exception e) {
//                System.out.println("e = " + e + "  adomain:" + adomain);
            }

            @Override
            public String handler(Adomain adomain) {
                return "handler ok";
            }
        };


        BatchExecute<Adomain, String> execute = new BatchExecute<>(handler, 199);
        execute.setDebug(false).execute(adomains);
    }
}
