package com.mapr.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanAllRows {

    public static void getAllRows(HTable table) throws IOException {

        Scan s = new Scan();
        s.setMaxVersions(10);

        ResultScanner scanner = table.getScanner(s);

        try {
            for (Result rr : scanner) {
                byte[] key = rr.getRow();

                //get value first column
                String inValue1 = Bytes.toString(rr.value());

                //get value by ColumnFamily and ColumnName
                byte[] inValueByte = rr.getValue(Bytes.toBytes("info"),
                    Bytes.toBytes("activeCode"));
                String inValue2 = Bytes.toString(inValueByte);

                //loop for result
                for (Cell cell : rr.listCells()) {
                    String qualifier =
                        Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.printf("Qualifier : %s : Value : %s", qualifier,
                        value);
                }
            }
        } finally {
            // Make sure scanners are closed. That's why it is done in finally
            // clause.
            scanner.close();
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try {
            HTable dataTable =
                new HTable(config, "/user/user01/datatable");
            ScanAllRows.getAllRows(dataTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}