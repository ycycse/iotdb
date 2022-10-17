package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PerformanceTest {
  static String USERNAME = "root";
  static String PASSWORD = "root";
  static String PORT = "6671";
  static String IP = "127.0.0.1";
  static String STORAGE_GROUP = "root.performance";
  static String DEVICE_ID = STORAGE_GROUP + ".test";

  static Session session = new Session(IP, PORT, USERNAME, PASSWORD);

  enum Type {
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    TEXT
  }

  public static void addMultiData(int timeSeriesNum, int dataNum)
      throws IoTDBConnectionException, StatementExecutionException {
    Session anotherSession = new Session(IP, 6669, USERNAME, PASSWORD);
    anotherSession.open();
    List<MeasurementSchema> schemaList = new ArrayList<>();

    for (int i = 0; i < timeSeriesNum; i++) {
      schemaList.add(new MeasurementSchema("testInt" + i, TSDataType.INT32));
      schemaList.add(new MeasurementSchema("testLong" + i, TSDataType.INT64));
      schemaList.add(new MeasurementSchema("testFloat" + i, TSDataType.FLOAT));
      schemaList.add(new MeasurementSchema("testDouble" + i, TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("testBoolean" + i, TSDataType.BOOLEAN));
      schemaList.add(new MeasurementSchema("testText" + i, TSDataType.TEXT));
    }

    Tablet tablet = new Tablet(DEVICE_ID, schemaList, 100);

    long timestamp = System.currentTimeMillis();
    for (int row = 0; row < dataNum; row++) {
      int r = tablet.rowSize++;

      for (int i = 0; i < timeSeriesNum; i++) {
        long LONG_DATA = new Random().nextLong();
        int INT_DATA = new Random().nextInt();
        float FLOAT_DATA = new Random().nextFloat();
        double DOUBLE_DATA = new Random().nextDouble();
        boolean BOOLEAN_DATA = new Random().nextBoolean();
        String TEXT_DATA = RandomStringUtils.random(10);

        tablet.addTimestamp(r, timestamp);
        tablet.addValue(schemaList.get(6 * i).getMeasurementId(), r, INT_DATA);
        tablet.addValue(schemaList.get(6 * i + 1).getMeasurementId(), r, LONG_DATA);
        tablet.addValue(schemaList.get(6 * i + 2).getMeasurementId(), r, FLOAT_DATA);
        tablet.addValue(schemaList.get(6 * i + 3).getMeasurementId(), r, DOUBLE_DATA);
        tablet.addValue(schemaList.get(6 * i + 4).getMeasurementId(), r, BOOLEAN_DATA);
        tablet.addValue(schemaList.get(6 * i + 5).getMeasurementId(), r, TEXT_DATA);
      }

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        anotherSession.insertTablet(tablet);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      anotherSession.insertTablet(tablet);
      tablet.reset();
    }
    anotherSession.close();
  }
  /**
   * randomly insert the same type of timeSeries
   *
   * @param type the type of timeSeries
   * @param timeSeriesNum the number of timeSeries
   * @param dataNum the row of data to be inserted
   */
  public static void addData(Type type, int timeSeriesNum, int dataNum)
      throws IoTDBConnectionException, StatementExecutionException {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    Session anotherSession = new Session(IP, 6669, USERNAME, PASSWORD);
    anotherSession.open();
    for (int i = 0; i < timeSeriesNum; i++) {
      switch (type) {
        case INT32:
          schemaList.add(new MeasurementSchema("testInt" + i, TSDataType.INT32));
          break;
        case INT64:
          schemaList.add(new MeasurementSchema("testLong" + i, TSDataType.INT64));
          break;
        case FLOAT:
          schemaList.add(new MeasurementSchema("testFloat" + i, TSDataType.FLOAT));
          break;
        case DOUBLE:
          schemaList.add(new MeasurementSchema("testDouble" + i, TSDataType.DOUBLE));
          break;
        case BOOLEAN:
          schemaList.add(new MeasurementSchema("testBoolean" + i, TSDataType.BOOLEAN));
          break;
        case TEXT:
          schemaList.add(new MeasurementSchema("testText" + i, TSDataType.TEXT));
      }
    }

    Tablet tablet = new Tablet(DEVICE_ID, schemaList, 100);
    long timestamp = System.currentTimeMillis();
    for (int row = 0; row < dataNum; row++) {
      int r = tablet.rowSize++;
      tablet.addTimestamp(r, timestamp);
      for (int i = 0; i < timeSeriesNum; i++) {
        tablet.addValue(schemaList.get(i).getMeasurementId(), r, getValue(type));
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        anotherSession.insertTablet(tablet);
        tablet.reset();
      }
      timestamp++;
    }
    if (tablet.rowSize != 0) {
      anotherSession.insertTablet(tablet);
      tablet.reset();
    }
    anotherSession.close();
  }

  static Object getValue(Type type) {
    switch (type) {
      case INT64:
        return new Random().nextLong();
      case INT32:
        return new Random().nextInt();
      case FLOAT:
        return new Random().nextFloat();
      case DOUBLE:
        return new Random().nextDouble();
      case BOOLEAN:
        return new Random().nextBoolean();
      case TEXT:
        return RandomStringUtils.random(10);
      default:
        return null;
    }
  }

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    int timeSeriesNum = Integer.parseInt(args[0]);
    int dataNum = Integer.parseInt(args[1]);
    int choice = Integer.parseInt(args[2]);

    //        addMultiData(timeSeriesNum, dataNum);
    //       addData(Type.INT64,12,1000);
    if (choice == 1) System.out.println("cost time: " + test2());
    else System.out.println("cost + traversal" + test1());
  }

  static double test1() throws IoTDBConnectionException {
    session.open();
    try {
      long newStart = System.nanoTime();
      OldSessionDataSet sessionDataSet =
          session.oldexecuteStatement("select * from " + DEVICE_ID, 10000);
      while (sessionDataSet.hasNext()) sessionDataSet.next();
      long newEnd = System.nanoTime();
      session.close();
      return (double) (newEnd - newStart) / 1e6;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  static double test2() throws IoTDBConnectionException {
    session.open();
    try {
      long newStart = System.nanoTime();
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select * from " + DEVICE_ID, 1000);
      long newEnd = System.nanoTime();

      while (sessionDataSet.hasNext()) {
        System.out.println(sessionDataSet.next());
        break;
      }
      session.close();
      return (double) (newEnd - newStart) / 1e6;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
