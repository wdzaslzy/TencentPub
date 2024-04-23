package com.tencent.hbase.demo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class PutDemo {


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.19.0.9,172.19.0.105,172.19.0.3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-emr-4vwm79d2");

        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(
                TableName.valueOf("lizy_test"))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"))
            .build();

        System.out.print("Creating table. ");
        if (admin.tableExists(tableDescriptor.getTableName())) {
            admin.disableTable(tableDescriptor.getTableName());
            admin.deleteTable(tableDescriptor.getTableName());
        }
        admin.createTable(tableDescriptor);

        Table table = connection.getTable(TableName.valueOf("lizy_test"));
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
            Bytes.toBytes("lizy"));
        table.put(put);

        System.out.println(" Done.");

        table.close();
        connection.close();
    }

}
