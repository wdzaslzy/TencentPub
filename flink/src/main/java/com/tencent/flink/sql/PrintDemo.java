package com.tencent.flink.sql;

import java.io.File;
import java.io.IOException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

public class PrintDemo {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String contents =
                "1,adfadfadsfadfadfsadfasd,3,2019-12-12 00:00:01\n";
//                        + "1,diaper,4,2019-12-12 00:00:02\n"
//                        + "1,pen,3,2019-12-12 00:00:04\n"
//                        + "1,adfadfadsfadfadfsadfasd,3,2019-12-12 00:00:06\n"
//                        + "1,rubber2,2,2019-12-12 00:00:10\n"
//                        + "1,rubber,2,2019-12-12 00:00:03\n"
//                        + "1,beer,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        String ddl =
                "CREATE TABLE orders (\n"
                        + "  user_id INT,\n"
                        + "  product STRING,\n"
                        + "  amount INT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";

        String printDdl = "CREATE TABLE print_orders (\n"
            + "  user_id INT,\n"
            + "  product STRING,\n"
            + "  amount INT,\n"
            + "  ts TIMESTAMP(3),\n"
            + "  WATERMARK FOR ts AS ts\n"
            + ") WITH (\n"
            + "  'connector' = 'print'\n"
            + ")";

        tEnv.executeSql(ddl);
        tEnv.executeSql(printDdl);
        tEnv.executeSql("insert into print_orders select user_id, if(product is null,'1111',product), amount, ts from orders;");
    }

    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("orders", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }

}
