package com.tencent.flink.java;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class GCOverheadOOMDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10_000);

        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("a_" + Math.random());
                    Thread.sleep(10_000L);
                }
            }

            @Override
            public void cancel() {

            }
        }).map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {

            }

            @Override
            public String map(String s) throws Exception {
                ExecutorService executor = Executors.newFixedThreadPool(5);
                for (int i = 0; i < Integer.MAX_VALUE; i++) {
                    executor.execute(() -> {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            //do nothing
                        }
                    });
                }
                return s;
            }
        }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                //...
            }
        });

        env.execute();

    }


}
