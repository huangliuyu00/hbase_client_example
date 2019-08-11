package com.liuyu.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SingleColumnValueFilterExample {
    public static void main(String[] args) throws IOException {
        //获取HBase配置信息
        Configuration configuration = HBaseConfiguration.create();
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        //初始化HBase 表
        Table table=conn.getTable(TableName.valueOf("tableName"));

        Scan scan = new Scan();
        //列值过滤器
        //列族
        String family="family";
        //列
        String qualifier="qualifier";
        //比较器，正则比较器，以'liuyu'开头的字符串
        RegexStringComparator comparator=new RegexStringComparator("liuyu.");
        //设定值与以'liuyu'开头字符串 相等 CompareOperator.EQUAL
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family.getBytes(),
                qualifier.getBytes(), CompareOperator.EQUAL, comparator);
        scan.setFilter(singleColumnValueFilter);
        ResultScanner resultScanner=table.getScanner(scan);
        for(Result result: resultScanner){
            String rowkey = Bytes.toString(result.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+":"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }
}
