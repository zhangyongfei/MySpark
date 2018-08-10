package org.apache.hadoop.hive.hbase.phoenix;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.phoenix.model.Column;
import org.apache.hadoop.hive.hbase.phoenix.model.Table;

import java.util.ArrayList;
import java.util.List;

public class HbaseTableUtil {

    public static Table getTable(String ddl, String mapping) {
        List<Column> columnList = new ArrayList<>();
        String columnString = ddl.substring(ddl.indexOf("{") + 1, ddl.indexOf("}")).trim();
        for (String columnAndDataType : columnString.split(",")) {
            String[] columnTmp = columnAndDataType.trim().split(" ");
            String dataType = columnTmp[0];
            String columnName = columnTmp[1];
            Column column = new Column(columnName, dataType);
            String[] arr = getColumnAndFamily(columnName, mapping);
            if (null == arr || (arr.length == 2 && arr[0].isEmpty())) {
                column.setRowKey(true);
            } else {
                column.setColumnFamily(arr[0]);
            }
            columnList.add(column);
        }
        return new Table(columnList);
    }

    private static String[] getColumnAndFamily(String columnName, String mapping) {
        for (String key : mapping.trim().split(",")) {
            String[] arr = key.trim().split(":");
            if (arr[1].replaceAll("#b|#s","").equalsIgnoreCase(columnName)) {
                return arr;
            }
        }
        return null;
    }
}
