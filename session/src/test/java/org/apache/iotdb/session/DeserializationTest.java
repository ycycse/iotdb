package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.IoTDBRpcDataSet;
import org.apache.iotdb.rpc.OldIoTDBRpcDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.OldSessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.sql.Timestamp;

public class DeserializationTest {
  static int[] dataNums = new int[]{
          1,
          1000,
          10000,
          100000,
          200000,
          300000,
          400000,
          500000,
          600000,
          700000,
          800000,
          900000,
          1000000
  };
  static String sql = "select s_0 from root.deserializationTest.node";
  public static void main(String[] args) throws InterruptedException {
    for(int i=0;i<dataNums.length;i++){
      System.out.println("dataNums:"+dataNums[i]);
      String temp = sql + " where Time<2022-11-17T16:06:44.395+08:00+"+dataNums[i];
      System.out.println("new:");
      testTimeForNew(temp);

      System.out.println("old:");
      testTimeForOld(temp);
    }
  }

  private static void testTimeForOld(String sql){

    try (Session session = new Session("127.0.0.1", 6667, "root", "root")) {
      long totalRecordTime = 0;
      long totalRowTime = 0;
      session.open(false);
      for(int i=0;i<120;i++){
        OldSessionDataSet oldSessionDataSet = session.oldexecuteStatement(sql, 500000);
        while (oldSessionDataSet.hasNext()) oldSessionDataSet.next();
        if(i<20)continue;//warmup
        totalRecordTime += oldSessionDataSet.getConstructRecordTime();
        totalRowTime += oldSessionDataSet.getConstructRowTime();
      }

      System.out.println(totalRecordTime);
      System.out.println(totalRowTime);


    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }
  private static void testTimeForNew(String sql){
    try (Session session = new Session("127.0.0.1", 6667, "root", "root")) {
      session.open();
      long totalRecordTime = 0;
      long totalRowTime = 0;
      long totalTsBlcokTime = 0;

      for(int i=0;i<120;i++){
        SessionDataSet sessionDataSet = session.executeQueryStatement(sql,100000);
        while(sessionDataSet.hasNext())sessionDataSet.next();
        if(i<20)continue;//warm up
        totalRecordTime += sessionDataSet.getConstructRecordTime();
        totalRowTime += sessionDataSet.getConstructRowTime();
        totalTsBlcokTime += sessionDataSet.getConstructTsBlockTime();
      }
      System.out.println(totalRecordTime);
      System.out.println(totalRowTime);
      System.out.println(totalTsBlcokTime);
    }catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }
}
