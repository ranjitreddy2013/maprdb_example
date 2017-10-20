package com.mapr.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTable {
    public static void createTable(Configuration conf, String tableName,
        String colFamilyPrefix) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor des = new HTableDescriptor(tableName.getBytes());
        HColumnDescriptor colDes = new HColumnDescriptor(colFamilyPrefix);
        colDes.setMaxVersions(100);
        colDes.setMinVersions(1);
        des.addFamily(colDes);

        try {
            admin.createTable(des);
            System.out.println("Created table " + tableName);
        } catch (TableExistsException te) {
            System.out.println("createTable threw exception " + te);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try {
            createTable(config, "/user/user01/datatable", "cf");
        } catch (Exception e) {
            System.out.println("Creating table threw exception " + e);
        }
    }
}
