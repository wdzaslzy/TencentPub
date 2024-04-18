package com.tencent.hbase.demo;

import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class HBaseThriftExample {

  public static void main(String args[]) throws Exception{
    // 创建链接
    TSocket socket = new TSocket("hbase-thrift-endpoint", 1234);
    TTransport transport = new TFramedTransport(socket);
    TProtocol protocol = new TBinaryProtocol(transport);

    THBaseService.Iface client = new THBaseService.Client(protocol);
    transport.open();

    // 定义表信息
    ByteBuffer table = ByteBuffer.wrap("TableName".getBytes());
    ByteBuffer row = ByteBuffer.wrap("rowkey".getBytes());
    ByteBuffer family = ByteBuffer.wrap("f".getBytes());
    ByteBuffer qualifier = ByteBuffer.wrap("col1".getBytes());
    ByteBuffer value = ByteBuffer.wrap("test-column-value".getBytes());

    // 写数据
    System.out.println("---put or update a key---");
    TPut put = new TPut();
    put.setRow(row);
    TColumnValue colVal = new TColumnValue(family, qualifier, value);
    put.setColumnValues(Arrays.asList(colVal));
    client.put(table, put);

    // 按rowkey读数据
    System.out.println("---get a row---");
    TGet get = new TGet();
    get.setRow(row);
    TColumn col = new TColumn()
            .setFamily(family)  // 不指定列簇,即是获取所有列簇
            .setQualifier(qualifier); // 不指定列名,即是获取所有列
    get.setColumns(Arrays.asList(col));
    TResult result = client.get(table, get);
    //System.out.println("get row : " + new String(result.getRow()));

    result.getColumnValues().forEach(c -> {
      System.out.println(new String(result.getRow()) + "  " + new String(c.getFamily()) + "_" +
              new String(c.getQualifier()) + ":" + new String(c.getValue()));
    });

    // scan数据
    System.out.println("---scan a table---");
    TScan scan = new TScan();
    List<TResult> resultList = client.getScannerResults(table, scan, 2);
    resultList.forEach(r -> r.getColumnValues().forEach(c -> {
      System.out.println(new String(r.getRow()) + "  " + new String(c.getFamily()) + "_" +
              new String(c.getQualifier()) + ":" + new String(c.getValue()));
    }));

    // 关闭链接
    transport.close();
    socket.close();
  }

}
