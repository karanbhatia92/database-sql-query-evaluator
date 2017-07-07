import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Mugdha on 7/2/2017.
 */
public class SubselectEvaluator implements Operator {
    ArrayList<PrimitiveValue[]> outputTupleList;
    int tupleLocation;
    ArrayList<Column> schemaList;
    Column[] schema;
    String alias;
    PlainSelect plainSelect;
    HashMap<String, CreateTable> createTableMap;

    public SubselectEvaluator(PlainSelect plainSelect, HashMap createTableMap, String alias){
        this.plainSelect = plainSelect;
        this.createTableMap = createTableMap;
        this.alias = alias;
        outputTupleList = new ArrayList<>();
        schemaList = new ArrayList<>();
        tupleLocation = 0;
    }

    public void execute(){
        SubMain subMain = new SubMain(this.plainSelect, this.createTableMap);
        outputTupleList = subMain.execute();
        Column[] tempSchema = subMain.schema;
        Table table = new Table("fromTable");
        table.setAlias(alias);

        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<ColumnDefinition> columnDefinitions = new ArrayList<>();
        for(int i = 0; i < tempSchema.length; i++){

            String tableName = tempSchema[i].getTable().getWholeTableName();
            CreateTable createTable = createTableMap.get(tableName);
            ArrayList<ColumnDefinition> tempColumnDefinition = (ArrayList<ColumnDefinition>) createTable.getColumnDefinitions();
            for(int j = 0; j < tempColumnDefinition.size(); j++){

                String columnName = tempColumnDefinition.get(j).getColumnName();
                if(columnName.equals(tempSchema[i].getColumnName())){
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
        createTableMap.put(createTable.getTable().getWholeTableName(),createTable);

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
