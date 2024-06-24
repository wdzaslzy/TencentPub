package com.tencent.flink.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.sink.HBaseMutationConverter;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class WriteHBaseExample {

    public static void main(String[] args) throws Exception {
        // 1. 准备hbase配置信息
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.0.16.10");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase");

        HBaseMutationConverter<Tuple2<String, Integer>> converter = new HBaseConverterImpl();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFunction<String>() {
                @Override
                public void run(SourceContext<String> ctx) throws Exception {

                    int index = 0;
                    long start = System.currentTimeMillis();
                    while (true) {
                        ctx.collect(
                            (int) (Math.random() * 500) + "_" + System.nanoTime() + "," + (int) (
                                Math.random() * 100));
                        index++;
                        if (index == 5000) {
                            index = 0;
                            long sub = System.currentTimeMillis() - start;
                            if (sub > 0) {
                                Thread.sleep(sub);
                            }
                            start = System.currentTimeMillis();
                        }
                    }
                }

                @Override
                public void cancel() {

                }
            }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String s) throws Exception {
                    String[] split = s.split(",");
                    return new Tuple2<>(split[0], Integer.parseInt(split[1]));
                }
            })
            .addSink(new HBaseSinkFunction<>("lizy_test", conf, converter, 2 * 1024 * 1024L, 10000,
                1000));

        env.execute();
    }

    private static class HBaseConverterImpl implements
        HBaseMutationConverter<Tuple2<String, Integer>> {

        @Override
        public void open() {

        }

        @Override
        public Mutation convertToMutation(Tuple2<String, Integer> tuple2) {
            Put put = new Put(Bytes.toBytes(tuple2.f0));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"),
                    Bytes.toBytes(tuple2.f1))
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("core"),
                    Bytes.toBytes(tuple2.f1))
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"),
                    Bytes.toBytes(tuple2.f0));
            return put;
        }
    }

}
