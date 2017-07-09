import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by Mugdha on 7/2/2017.
 */
public class SubselectEvaluator implements Operator {
    ArrayList<PrimitiveValue[]> outputTupleList;
    HashMap<String, Integer> databaseMap;
    int tupleLocation;
    ArrayList<Column> schemaList;
    Column[] schema;
    String alias;
    PlainSelect plainSelect;
    HashMap<String, CreateTable> createTableMap;
    HashSet<String> fromObjects;
    HashSet<String> groupObject;
    HashSet<String> orderObject;

    public SubselectEvaluator(PlainSelect plainSelect, HashMap createTableMap,
                              String alias, HashMap<String, Integer> databaseMap){
        this.plainSelect = plainSelect;
        this.createTableMap = createTableMap;
        this.databaseMap = databaseMap;
        this.alias = alias;
        outputTupleList = new ArrayList<>();
        schemaList = new ArrayList<>();
        tupleLocation = 0;
        fromObjects = new HashSet<>();
        groupObject = new HashSet<>();
        orderObject = new HashSet<>();
    }

    public void execute(){
        SubMain subMain = new SubMain(plainSelect, createTableMap, databaseMap);
        outputTupleList = subMain.execute();
        if(fromObjects != null){
            fromObjects = subMain.fromObjects;
        }
        if(groupObject != null){
            groupObject = subMain.groupObject;
        }
        if(orderObject != null){
            orderObject = subMain.orderObject;
        }
        Column[] tempSchema = subMain.newSchema;
        Table table = new Table("fromtable");
        table.setAlias(alias);

        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<ColumnDefinition> columnDefinitions = new ArrayList<>();
        for(int i = 0; i < tempSchema.length; i++){

            String tableName = tempSchema[i].getTable().getWholeTableName().toLowerCase();
            CreateTable createTable = createTableMap.get(tableName);
            ArrayList<ColumnDefinition> tempColumnDefinition = (ArrayList<ColumnDefinition>) createTable.getColumnDefinitions();
            for(int j = 0; j < tempColumnDefinition.size(); j++){

                String columnName = tempColumnDefinition.get(j).getColumnName().toLowerCase();
                if(columnName.equals(tempSchema[i].getColumnName().toLowerCase())){
                    if(!(columnList.contains(columnName))){
                        columnList.add(columnName);
                        columnDefinitions.add(tempColumnDefinition.get(j));
                        schemaList.add(tempSchema[i]);
                    }
                }
            }
        }
        CreateTable createTable = new CreateTable();
        createTable.setTable(table);
        createTable.setColumnDefinitions(columnDefinitions);
        createTableMap.put(createTable.getTable().getWholeTableName().toLowerCase(),createTable);

        for(int i = 0; i < schemaList.size(); i++){
            schemaList.get(i).setTable(table);
        }
        schema = new Column[schemaList.size()];
        schema = schemaList.toArray(schema);
    }
    public PrimitiveValue[] readOneTuple(){
        PrimitiveValue[] tuple;
        if(tupleLocation < outputTupleList.size()) {
            tuple = outputTupleList.get(tupleLocation);
            tupleLocation++;
        } else {
            tuple = null;
        }
        return tuple;
    }
    public void reset(){
        tupleLocation = 0;
    }
}
