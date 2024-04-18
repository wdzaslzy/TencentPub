package com.tencent.hbase.demo;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

public class ThriftScanMeta {

    public static void main(String[] args) throws Exception {
        TSocket socket = new TSocket(args[0], Integer.parseInt(args[1]));
        TTransport transport = new TFramedTransport(socket, 1213486160);
        TProtocol protocol = new TBinaryProtocol(transport);

        THBaseService.Iface client = new THBaseService.Client(protocol);
        transport.open();

        System.out.println("---scan a table---");
        TScan scan = new TScan();
        ByteBuffer table = ByteBuffer.wrap("hbase:meta".getBytes());
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
