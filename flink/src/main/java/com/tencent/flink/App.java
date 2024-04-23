package com.tencent.flink;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.flink.connector.postgre.internal.executor.TableCopyStatementExecutor;

public class App {

    public static void main(String[] args) {
//        int index = 0;
//        try {
//            if (index == 0) {
//                throw new RuntimeException("failed");
//            } else {
//                throw new SQLException();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            int a = 1 / 0;
//            System.out.println(a);
//        }
        test();
    }

    private static void test(){
        FutureTask<Long> copyResult = new FutureTask(new Callable<Long>() {
            public Long call() throws Exception {
                int index = 0;
                try {
                    if (index == 0) {
                        throw new RuntimeException("failed");
                    } else {
                        throw new SQLException();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("final");
                }
                return 0L;
            }
        });

        Executors.newFixedThreadPool(1).submit(copyResult);

        int a = 1/0;

        try {
            Long aLong = copyResult.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

    }

}
