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
import java.util.*;


public class Main {
    
    public static void main(String[] args) {

        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String, Integer> databaseMap = new HashMap<>();
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
                            for (int i = 0; i < columnDefinitionList.size(); i++) {
                                String colName = columnDefinitionList.get(i).getColumnName().toLowerCase();
                                databaseMap.put(tableName + "." +colName, i);
                            }
                            createTableMap.put(tableName, ct);
                        } else if (stmt instanceof Select) {
                            SelectBody selectBody = ((Select)stmt).getSelectBody();

                            if(selectBody instanceof Union) {
                                Union union = (Union)selectBody;
                                List<PlainSelect> plainSelectsList = union.getPlainSelects();
                                HashMap<String, Integer> columnIndex = new HashMap<>();
                                HashSet<String> unionResult = new HashSet<String>();
                                Column[] tempSchema = null;

                                for(int i = 0; i < plainSelectsList.size(); i++) {
                                    PlainSelect plainSelectStmt = plainSelectsList.get(i);
                                    SubMain subMain = new SubMain(plainSelectStmt, createTableMap, databaseMap);
                                    ArrayList<PrimitiveValue[]> plainSelectResult = subMain.execute();        //Memory Wastage
                                    Column[] selectSchema = subMain.newSchema;

                                    if(selectSchema != null) {
                                        if(tempSchema == null) {
                                            tempSchema = selectSchema;
                                            for(int j = 0; j < tempSchema.length; j++) {
                                                columnIndex.put(tempSchema[j].getWholeColumnName().toLowerCase(), j);
                                            }
                                            Iterator<PrimitiveValue[]> iterator = plainSelectResult.iterator();
                                            while(iterator.hasNext()) {
                                                PrimitiveValue[] value = iterator.next();
                                                String key = "";
                                                for(int k = 0; k < tempSchema.length; k++) {
                                                    key = key + value[k].toString() + " | ";
                                                }
                                                if(unionResult.add(key)) {
                                                    System.out.println(key);
                                                }
                                                iterator.remove();
                                            }
                                        }else {
                                            ArrayList<Integer> indexOrder = new ArrayList<Integer>();
                                            for(int j = 0; j < tempSchema.length; j++) {
                                                indexOrder.add(columnIndex.get(selectSchema[j].getWholeColumnName().toLowerCase()));
                                            }
                                            Iterator<PrimitiveValue[]> iterator = plainSelectResult.iterator();
                                            while(iterator.hasNext()) {
                                                PrimitiveValue[] value = iterator.next();
                                                String key = "";
                                                for(int k = 0; k < tempSchema.length; k++) {
                                                    key = key + value[indexOrder.get(k)].toString() + " | ";
                                                }
                                                if(unionResult.add(key)) {
                                                    System.out.println(key);
                                                }
                                                iterator.remove();
                                            }
                                        }
                                    }
                                }
                                if(unionResult.isEmpty()) {
                                    System.out.println("NULL");
                                }
                            }else {
                                PlainSelect plainSelect = (PlainSelect)selectBody;
                                SubMain subMain = new SubMain(plainSelect, createTableMap, databaseMap);
                                outputTupleList = subMain.execute();

                                for(int i = 0; i < outputTupleList.size(); i++){
                                    PrimitiveValue[] pv = outputTupleList.get(i);
                                    for(int j = 0; j < pv.length; j++){
                                        System.out.print(pv[j] + "|" );
                                    }
                                    System.out.println();
                                }
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

