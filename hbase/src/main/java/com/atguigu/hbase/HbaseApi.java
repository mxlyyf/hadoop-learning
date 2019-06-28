package com.atguigu.hbase;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseApi {
    public static final Logger log = LoggerFactory.getLogger(HbaseApi.class);
    public static Connection conn;
    public static ThreadLocal<Admin> adminTL;
    public static ThreadLocal<Map<String, Table>> tableTL;

    static {
        try {
            // HBaseConfiguration.create()：创建一个configuration，读取hbase的配置文件！
            conn = ConnectionFactory.createConnection();
            adminTL = new ThreadLocal<>();
            tableTL = new ThreadLocal<>();
            tableTL.set(new HashedMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取admin
    public static Admin getAdmin() throws IOException {
        Admin admin = adminTL.get();
        if (admin == null) {
            admin = conn.getAdmin();
            adminTL.set(admin);
        }
        return admin;
    }

    //判断表是否存在
    public static boolean ifTableExists(String ns, String tablename) throws IOException {
        if (StringUtils.isEmpty(tablename)) {
            log.error("tablename不能为null");
            return false;
        }
        return getAdmin().tableExists(buildTableName(ns, tablename));
    }

    //获取一个table
    public static Table getTable(String ns, String tablename) throws Exception {
        if (!ifTableExists(ns, tablename)) {
            log.error("table 不存在");
            return null;
        }
        Map<String, Table> tableMap = tableTL.get();
        TableName tn = buildTableName(ns,tablename);
        Table table = tableMap.get(Bytes.toString(tn.getName()));
        //判断table是否之前已经创建
        if (table == null) {
            table = conn.getTable(tn);
            tableMap.put(Bytes.toString(tn.getName()), table);
            tableTL.set(tableMap);
        }
        return table;
    }

    // put操作:  put '表名'，'rowkey','列族：列名', value, [ts]
    public static void putData(String ns, String tablename, String rowkey, String family, String column, String value) throws Exception {
        //put对象，代表添加一行数据
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        getTable(ns, tablename).put(put);
    }

    //delete操作：delete  'ns:tb','rk'
    public static void deleteData(String ns, String tablename, String rk) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rk));
        getTable(ns, tablename).delete(delete);
    }

    //get操作: get 'ns:tb','rk',{[],...}
    public static Result getData(String ns, String tablename, String rk) throws Exception {
        Get get = new Get(Bytes.toBytes(rk));
        return getTable(ns, tablename).get(get);
    }

    //scan操作：scan 'ns:tb'
    public static List<Result> scanData(String ns, String tablename) throws Exception {
        List results = new ArrayList();
        ResultScanner scanner = getTable(ns, tablename).getScanner(new Scan());
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            results.add(next);
        }
        return results;
    }

    //
    public static TableName buildTableName(String ns, String tablename){
        TableName tn;
        if (StringUtils.isEmpty(ns)) {
            tn = TableName.valueOf(tablename);
        } else {
            tn = TableName.valueOf(ns, tablename);
        }
        return tn;
    }

    //释放资源
    public static void close() throws Exception {
        //1.关闭Admin
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminTL.remove();
        }
        //2.关闭Table
        Collection<Table> tables = tableTL.get().values();
        for (Table table : tables
        ) {
            table.close();
        }
        tableTL.remove();
        //3.关闭conn
        if (conn != null) conn.close();
    }

    public static void main(String[] args) throws Exception {
        //putData("", "student", "1004", "info", "sex", "male");
        List<Result> results = scanData("", "student");
        log.info(results.toString());
        close();
    }
}
