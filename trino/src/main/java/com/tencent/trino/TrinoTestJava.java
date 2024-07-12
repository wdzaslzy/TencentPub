package com.tencent.trino;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TrinoTestJava {

    public static void main(String[] args) {
        String url = "jdbc:trino://42.194.adsf.31:8443/hive/default";

        try {
            Properties properties = new Properties();
            properties.setProperty("user", "root");
            properties.setProperty("password", "dsf@");
            properties.setProperty("SSL", "true");
            properties.setProperty("SSLKeyStorePath",
                "/Users/rodenli/Workspace/trino_keystore.jks");
            properties.setProperty("SSLKeyStorePassword", "trino@123");
            Connection connection = DriverManager.getConnection(url, properties);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("show tables");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            rs.close();
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


}
