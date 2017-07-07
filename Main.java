import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Main {
    
    public static void main(String[] args) {

        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String, HashMap<String, ColumnIdType>> databaseMap = new HashMap<>();
        HashMap<String, CreateTable> createTableMap = new HashMap<>();
        String workingDir = System.getProperty("user.dir");
        System.out.println(workingDir);
        File dir = new File(workingDir);
        File[] directoryListing = dir.listFiles();
        ArrayList<PrimitiveValue[]> outputTupleList;

        for (File child : directoryListing) {
            if (child.isFile() && child.getName().endsWith(".sql")) {
                sqlFiles.add(child);
                try {
                    FileReader stream = new FileReader(child);
                    CCJSqlParser parser = new CCJSqlParser(stream);
                    Statement stmt;
                    while ((stmt = parser.Statement()) != null) {
                        if (stmt instanceof CreateTable) {
                            CreateTable ct = (CreateTable)stmt;
                            String tableName = ct.getTable().getName().toLowerCase();
                            List<ColumnDefinition> columnDefinitionList = ct.getColumnDefinitions();
                            HashMap<String, ColumnIdType> tableMap = new HashMap<>();
                            for (int i = 0; i < columnDefinitionList.size(); i++) {
                                String colName = columnDefinitionList.get(i).getColumnName().toLowerCase();
                                ColDataType colDataType = columnDefinitionList.get(i).getColDataType();
                                ColumnIdType columnIdType = new ColumnIdType(i, colDataType.getDataType());
                                tableMap.put(colName, columnIdType);
                            }
                            databaseMap.put(tableName, tableMap);
                            createTableMap.put(tableName, ct);
                        } else if (stmt instanceof Select) {
                            SelectBody selectBody = ((Select)stmt).getSelectBody();

                            if(selectBody instanceof Union) {
                                Union union = (Union)selectBody;
                                List<PlainSelect> plainSelectsList = union.getPlainSelects();
                                ArrayList<PrimitiveValue[]> tempUnion = new ArrayList<PrimitiveValue[]>();
                                Column[] tempSchema = null;
                                int i = 0;
                                HashMap<String, PrimitiveValue[]> tempHashMap = new HashMap<String, PrimitiveValue[]>();
                                Column[] tempSchema2 = null;
                                ArrayList<PrimitiveValue[]> plainSelectResult2 = null;
                                HashMap<String, Integer> schemaIndex = new HashMap<>();

                                while(i < plainSelectsList.size()) {
                                    if (i == 0) {
                                        PlainSelect plainSelectStmt = plainSelectsList.get(i);
                                        SubMain subMain = new SubMain(plainSelectStmt, createTableMap);
                                        ArrayList<PrimitiveValue[]> plainSelectResult1 = subMain.execute();
                                        tempSchema = subMain.newSchema;

                                        i++;

                                        plainSelectStmt = plainSelectsList.get(i);
                                        subMain = new SubMain(plainSelectStmt, createTableMap);
                                        plainSelectResult2 = subMain.execute();
                                        tempSchema2 = subMain.newSchema;

                                        i++;

                                        if(tempSchema == null && tempSchema2 != null){
                                            tempSchema = tempSchema2;
                                            for(int j = 0; j < tempSchema.length; j++) {
                                                schemaIndex.put(tempSchema[j].getWholeColumnName().toLowerCase(), j);
                                            }
                                            for (int j = 0; j < plainSelectResult2.size(); j++) {
                                                String key = "";
                                                PrimitiveValue[] value = plainSelectResult2.get(j);
                                                for(int k = 0; k < tempSchema.length; k++) {
                                                    key = key + value[k].toString();
                                                }
                                                if (tempHashMap.get(key) == null) {
                                                    tempUnion.add(value);
                                                    tempHashMap.put(key, value);
                                                }
                                            }

                                        }else if(tempSchema != null && tempSchema2 == null) {
                                            for(int j = 0; j < tempSchema.length; j++) {
                                                schemaIndex.put(tempSchema[j].getWholeColumnName().toLowerCase(), j);
                                            }
                                            for (int j = 0; j < plainSelectResult1.size(); j++) {
                                                String key = "";
                                                PrimitiveValue[] value = plainSelectResult1.get(j);
                                                for(int k = 0; k < tempSchema.length; k++) {
                                                    key = key + value[k].toString();
                                                }
                                                if (tempHashMap.get(key) == null) {
                                                    tempUnion.add(value);
                                                    tempHashMap.put(key, value);
                                                }
                                            }
                                        }else if(tempSchema != null && tempSchema2 != null){
                                            assert tempSchema.length==tempSchema2.length : "Union: schema1 length does not match schema2 length";
                                            for(int j = 0; j < tempSchema.length; j++) {
                                                schemaIndex.put(tempSchema[j].getWholeColumnName().toLowerCase(), j);
                                            }
                                            for (int j = 0; j < plainSelectResult1.size(); j++) {
                                                String key = "";
                                                PrimitiveValue[] value = plainSelectResult1.get(j);
                                                for(int k = 0; k < tempSchema.length; k++) {
                                                    key = key + value[k].toString();
                                                }
                                                if (tempHashMap.get(key) == null) {
                                                    tempUnion.add(value);
                                                    tempHashMap.put(key, value);
                                                }
                                            }
                                            ArrayList<Integer> indexOrder = new ArrayList<Integer>();
                                            for(int j = 0; j < tempSchema2.length; j++) {
                                                indexOrder.add(schemaIndex.get(tempSchema2[j].getWholeColumnName().toLowerCase()));
                                            }

                                            for(int j = 0 ; j < plainSelectResult2.size(); j++) {
                                                PrimitiveValue[] value = plainSelectResult2.get(j);
                                                String key = "";
                                                for(int k = 0; k < tempSchema2.length; k++) {
                                                    key = key + value[indexOrder.get(k)].toString();
                                                }
                                                if (tempHashMap.get(key) == null) {
                                                    tempHashMap.put(key, value);
                                                    tempUnion.add(value);
                                                }
                                            }
                                        }
                                    }else {
                                        PlainSelect plainSelectStmt = plainSelectsList.get(i);
                                        SubMain subMain = new SubMain(plainSelectStmt, createTableMap);
                                        plainSelectResult2 = subMain.execute();
                                        tempSchema2 = subMain.newSchema;

                                        i++;
                                        if(tempSchema == null && tempSchema2 != null) {

                                            tempSchema = tempSchema2;
                                            if(schemaIndex.isEmpty()){
                                                for(int j = 0; j < tempSchema.length; j++) {
                                                    schemaIndex.put(tempSchema[j].getWholeColumnName().toLowerCase(), j);
                                                }
                                            }
                                            for (int j = 0; j < plainSelectResult2.size(); j++) {
                                                String key = "";
                                                PrimitiveValue[] value = plainSelectResult2.get(j);
                                                for(int k = 0; k < tempSchema.length; k++) {
                                                    key = key + value[k].toString();
                                                }
                                                if (tempHashMap.get(key) == null) {
                                                    tempUnion.add(value);
                                                    tempHashMap.put(key, value);
                                                }
                                            }
                                        }else if(tempSchema != null && tempSchema2 != null){
                                            assert tempSchema.length==tempSchema2.length : "Union: schema1 length does not match schema2 length";
                                            ArrayList<Integer> indexOrder = new ArrayList<Integer>();
                                            for(int j = 0; j < tempSchema2.length; j++) {
                                                indexOrder.add(schemaIndex.get(tempSchema2[j].getWholeColumnName().toLowerCase()));
                                            }

                                            for(int j = 0 ; j < plainSelectResult2.size(); j++) {
                                                PrimitiveValue[] value = plainSelectResult2.get(j);
                                                String key = "";
                                                for(int k = 0; k < tempSchema2.length; k++) {
                                                    key = key + value[indexOrder.get(k)].toString();
                                                }
                                                if (tempHashMap.get(key) == null) {
                                                    tempHashMap.put(key, value);
                                                    tempUnion.add(value);
                                                }
                                            }
                                        }

                                    }
                                }
                                outputTupleList = tempUnion;
                            }else {
                                PlainSelect plainSelect = (PlainSelect)selectBody;
                                SubMain subMain = new SubMain(plainSelect, createTableMap);
                                outputTupleList = subMain.execute();
                            }

                            for(int i = 0; i < outputTupleList.size(); i++){
                                PrimitiveValue[] pv = outputTupleList.get(i);
                                for(int j = 0; j < pv.length; j++){
                                    System.out.print(pv[j] + "|" );
                                }
                                System.out.println();
                            }
                            System.out.println("---------------------------------------------------");
                            System.out.println();
                        } else {
                            System.out.println("PANIC: I don't know how to handle " + stmt);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

