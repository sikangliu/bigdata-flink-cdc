package com.lsk.flinkcdc;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT," +
                " name STRING," +
                " phone_num STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'hadoop102'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '000000'," +
                " 'database-name' = 'gmall-flink'," +
                " 'table-name' = 'z_user_info'" +
                ")");
        tableEnv.executeSql("select * from user_info").print();
        env.execute();
    }
}
