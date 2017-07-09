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
/*
        ArrayList<PrimitiveValue[]> outputTupleList = null;
        HashSet<String> fromObjects = new HashSet<>();
        HashSet<String> projectionObjects = new HashSet<>();
        HashSet<String> groupObject = new HashSet<>();
        HashSet<String> orderObject = new HashSet<>();
        HashSet<String> whereObjects = new HashSet<>();
        HashSet<String> joinObjects = new HashSet<>();
*/

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
                            ArrayList<PrimitiveValue[]> outputTupleList = null;
                            HashSet<String> fromObjects = new HashSet<>();
                            HashSet<String> projectionObjects = new HashSet<>();
                            HashSet<String> groupObject = new HashSet<>();
                            HashSet<String> orderObject = new HashSet<>();
                            HashSet<String> whereObjects = new HashSet<>();
                            HashSet<String> joinObjects = new HashSet<>();
                            SelectBody selectBody = ((Select)stmt).getSelectBody();

                            if(selectBody instanceof Union) {
                                Union union = (Union)selectBody;
                                List<PlainSelect> plainSelectsList = union.getPlainSelects();
                                HashMap<String, Integer> columnIndex = new HashMap<>();
                                HashSet<String> unionResult = new HashSet<String>();
                                Column[] tempSchema = null;
                                outputTupleList = new ArrayList<>();

                                for(int i = 0; i < plainSelectsList.size(); i++) {
                                    PlainSelect plainSelectStmt = plainSelectsList.get(i);
                                    SubMain subMain = new SubMain(plainSelectStmt, createTableMap, databaseMap);
                                    ArrayList<PrimitiveValue[]> plainSelectResult = subMain.execute();        //Memory Wastage
                                    Column[] selectSchema = subMain.newSchema;

                                    HashSet<String> tempfromObjects = subMain.fromObjects;
        							HashSet<String> tempprojectionObjects = subMain.projectionObjects;
        							HashSet<String> tempgroupObject = subMain.groupObject;
        							HashSet<String> temporderObject = subMain.orderObject;
        							HashSet<String> tempwhereObjects = subMain.whereObjects;
        							HashSet<String> tempjoinObjects = subMain.joinObjects;

        							if(!tempprojectionObjects.isEmpty()){
            							Iterator<String> iterator = tempprojectionObjects.iterator();
            							while(iterator.hasNext()){
                							projectionObjects.add(iterator.next());
            							}
        							}

        							if(!tempfromObjects.isEmpty()){
            							Iterator<String> iterator = tempfromObjects.iterator();
            							while(iterator.hasNext()){
                							fromObjects.add(iterator.next());
            							}
        							}

        							if(!tempgroupObject.isEmpty()){
            							Iterator<String> iterator = tempgroupObject.iterator();
            							while(iterator.hasNext()){
                							groupObject.add(iterator.next());
            							}
        							}

        							if(!temporderObject.isEmpty()){
            							Iterator<String> iterator = temporderObject.iterator();
            							while(iterator.hasNext()){
                							orderObject.add(iterator.next());
            							}
        							}

                                    if(!tempwhereObjects.isEmpty()){
                                        Iterator<String> iterator = tempwhereObjects.iterator();
                                        while(iterator.hasNext()){
                                            whereObjects.add(iterator.next());
                                        }
                                    }

                                    if(!tempjoinObjects.isEmpty()){
                                        Iterator<String> iterator = tempjoinObjects.iterator();
                                        while(iterator.hasNext()){
                                            joinObjects.add(iterator.next());
                                        }
                                    }

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
                                                    key = key + value[k].toString();
                                                }
                                                if(unionResult.add(key)) {
                                                    //System.out.println(key);
                                                    try {
                                                        outputTupleList.add(value);
                                                    } catch (NullPointerException e) {
                                                        e.printStackTrace();
                                                    }

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
                                                    key = key + value[indexOrder.get(k)].toString(); //mugdha
                                                }
                                                if(unionResult.add(key)) {
                                                   // System.out.println(key);
                                                    outputTupleList.add(value);
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
                                fromObjects = subMain.fromObjects;
                                projectionObjects = subMain.projectionObjects;
                                groupObject = subMain.groupObject;
                                orderObject = subMain.orderObject;
                                whereObjects = subMain.whereObjects;
                                joinObjects = subMain.joinObjects;

                            }
                            printQuery(stmt, outputTupleList, projectionObjects,
                                    groupObject, orderObject, fromObjects, whereObjects, joinObjects);
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

    public static void printQuery(Statement stmt, ArrayList outputTupleList, HashSet projectionObjects,
                                    HashSet groupObject, HashSet orderObject, HashSet fromObjects, HashSet whereObjects, HashSet joinObjects){

        System.out.println(stmt);

        if(!projectionObjects.isEmpty()){
            Iterator<String> iterator = projectionObjects.iterator();
            System.out.print("PROJECTION: " );
            while(iterator.hasNext()){
                System.out.print(" " + iterator.next() + " ");
            }
            System.out.println();
        }
        else{
            System.out.println("PROJECTION: NULL");
        }

        if(!fromObjects.isEmpty()){
            Iterator<String> iterator = fromObjects.iterator();
            System.out.print("FROM: " );
            while(iterator.hasNext()){
                System.out.print(" " + iterator.next() + " ");
            }
            System.out.println();
        }
        else{
            System.out.println("FROM: NULL");
        }
        //SELECTION and JOIN

        if(!joinObjects.isEmpty()){
            Iterator<String> iterator = joinObjects.iterator();
            System.out.print("JOIN: " );
            while(iterator.hasNext()){
                System.out.print(" " + iterator.next() + " ");
            }
            System.out.println();
        }
        else{
            System.out.println("JOIN: NULL");
        }

        if(!groupObject.isEmpty()){
            Iterator<String> iterator = groupObject.iterator();
            System.out.print("GROUP-BY: " );
            while(iterator.hasNext()){
                System.out.print(" " + iterator.next() + " ");
            }
            System.out.println();
        }
        else{
            System.out.println("GROUP-BY: NULL");
        }

        if(!orderObject.isEmpty()){
            Iterator<String> iterator = orderObject.iterator();
            System.out.print("ORDER-BY: " );
            while(iterator.hasNext()){
                System.out.print(" " + iterator.next() + " ");
            }
            System.out.println();
        }
        else{
            System.out.println("ORDER-BY: NULL");
        }

        if(!whereObjects.isEmpty()){
            Iterator<String> iterator = whereObjects.iterator();
            System.out.print("SELECTION: " );
            while(iterator.hasNext()){
                System.out.print(" " + iterator.next() + " ");
            }
            System.out.println();
        }
        else{
            System.out.println("SELECTION: NULL");
        }

        for(int i = 0; i < outputTupleList.size(); i++){
            PrimitiveValue[] pv = (PrimitiveValue[]) outputTupleList.get(i);
            for(int j = 0; j < pv.length; j++){
                System.out.print(pv[j] + "|" );
            }
            System.out.println();
        }
    }
}

