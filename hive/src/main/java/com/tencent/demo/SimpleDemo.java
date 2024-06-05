package com.tencent.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDemo {


    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException {
        try {
            // 加载hive-jdbc驱动
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // 根据连接信息和账号密码获取连接
        Connection conn = DriverManager.getConnection("jdbc:hive2://xx:7001", "hadoop", "");
        // 创建状态参数（使用conn.prepareStatement(sql)预编译sql防止sql注入，但常用于参数化执行sql，批量执行不同的sql建议使用下面这种方式）
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("select * from dm_browser_income_inc_d");

        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }


    }


}
