package com.tencent.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

public class WindowDemo {

    public static void main(String[] args) throws IOException {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String contents =
                "1,beer,3,2019-12-12 00:00:01\n"
                        + "1,diaper,4,2019-12-12 00:00:02\n"
                        + "1,pen,3,2019-12-12 00:00:04\n"
                        + "1,rubber,3,2019-12-12 00:00:06\n"
                        + "1,rubber,2,2019-12-12 00:00:10\n"
                        + "1,rubber,2,2019-12-12 00:00:03\n"
                        + "1,beer,1,2019-12-12 00:00:08";
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
        tEnv.executeSql(ddl);


        tEnv.executeSql(getWindowSQL()).print();
    }

    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("orders", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }

    private static String getWindowSQL(){
        String windowSQL = "select CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start, \n" +
                "count(*) as order_num from table (\n" +
                "   TUMBLE(\n" +
                "     DATA => TABLE orders,\n" +
                "     TIMECOL => DESCRIPTOR(ts),\n" +
                "     SIZE => INTERVAL '5' MINUTES))" +
                " GROUP BY TUMBLE(ts, INTERVAL '5' SECOND);";
        return windowSQL;
    }

}
