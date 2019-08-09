package com.liuyu.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author huangliuyu
 * @description
 * @date 2019-08-09
 */
public class HBaseService {
    private Configuration config;

    public void queryAll(String tableName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(config);
        Table table=conn.getTable(TableName.valueOf(tableName));
        //多个过滤器
        FilterList filterList=new FilterList();

        //分页过滤器
        PageFilter pageFilter=new PageFilter(10000);
        filterList.addFilter(pageFilter);

        //SingleColumnValueFilter
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("address"),
                Bytes.toBytes("city"), CompareOperator.EQUAL,
                new RegexStringComparator("hang.*"));
        filterList.addFilter(singleColumnValueFilter);






        Scan scan=new Scan();
        scan.setFilter(filterList);
        ResultScanner rsacn = table.getScanner(scan);
        for (Result rs : rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+":"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
        rsacn.close();
    }



    public Configuration getConfig() {
        return config;
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }
}
