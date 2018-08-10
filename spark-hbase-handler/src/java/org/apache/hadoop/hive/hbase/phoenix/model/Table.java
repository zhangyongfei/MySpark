package org.apache.hadoop.hive.hbase.phoenix.model;

import java.util.List;

public class Table {
    private List<Column> columnList;

    public Table() {
    }

    public Table(List<Column> columnList) {
        this.columnList = columnList;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public Column getRowKeyColumn() {
        for (Column column : columnList) {
            if (column.isRowKey()) {
                return column;
            }
        }
        return null;
    }

    public Column getColumn(String columnName) {
        for (Column column : columnList) {
            if (column.getColumnName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }
}
