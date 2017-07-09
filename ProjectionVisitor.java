import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by Karan on 6/28/2017.
 */
public class ProjectionVisitor implements SelectItemVisitor {

    public Column[] schema;
    public ArrayList<Integer> columnIndexes;
    HashMap<String, String> aliasHashMap;
    ArrayList<PrimitiveValue[]> tupleList;
    public ArrayList<PrimitiveValue[]> projectionVisitorOutList;
    public Column[] projectionSchema;
    Boolean projectionFlag;
    PlainSelect plainSelect;
    HashMap groupByMap;
    HashSet projectionObjects;
    HashSet<String> orderObject;
    HashMap<String, CreateTable> createTableMap;
    HashMap<String, Integer> databaseMap;

    public ProjectionVisitor(Column[] schema, HashMap<String, String> aliasHashMap,
                             Boolean projectionFlag, PlainSelect plainSelect,
                             ArrayList tupleList, HashMap groupByMap,
                             HashMap<String, CreateTable> createTableMap,
                             HashMap<String, Integer> databaseMap) {
        columnIndexes = new ArrayList<>();
        this.schema = schema;
        this.aliasHashMap = aliasHashMap;
        this.projectionFlag = projectionFlag;
        this.plainSelect = plainSelect;
        this.tupleList = tupleList;
        this.groupByMap = groupByMap;
        this.projectionSchema = null;
        projectionObjects = new HashSet();
        orderObject = new HashSet<>();
        this.databaseMap = databaseMap;
        this.createTableMap = createTableMap;
    }
    public void visit(AllColumns allColumns) {
        for(int i = 0; i < schema.length; i++) {
            columnIndexes.add(i);
        }
        FromItem fromItem = plainSelect.getFromItem();
        String tableName = "";
        if(fromItem instanceof Table){
            tableName = ((Table) fromItem).getWholeTableName();
            for(Column column : schema){
                //CHECK diff betwn column name and whole column name
                if(column.getTable().getWholeTableName().toLowerCase().equals(tableName)){
                    if(!projectionObjects.contains(column.getWholeColumnName())){
                        if(!projectionObjects.contains(column.getWholeColumnName())){
                            String c = column.getColumnName();
                            String t = ((Table) fromItem).getWholeTableName();
                            String m = t + "." + c;
                            projectionObjects.add(m);
                        }
                    }
                }
            }
        }
    }

    public void visit(AllTableColumns allTableColumns) {

        String aliasName = allTableColumns.getTable().getWholeTableName().toLowerCase();
        String tableName;
        if(aliasHashMap.containsKey(aliasName)){
            tableName = aliasHashMap.get(aliasName);
            for(int i = 0; i < schema.length; i++){
                if(schema[i].getTable().getWholeTableName().toLowerCase().equals(tableName)){
                    columnIndexes.add(i);
                    if(!projectionObjects.contains(schema[i].getWholeColumnName())){
                        String c = schema[i].getColumnName();
                        //String a = schema[i].getWholeColumnName();
                        String m = tableName + "." + c;
                        projectionObjects.add(m);
                    }
                }
            }
        }
    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        String tableName;
        String projectionAliasName = "";
        Expression orderByExp = null;
        Expression expression = selectExpressionItem.getExpression();

        if(expression instanceof Column) {
            Column column = (Column)expression;
            if(column.getTable().getName() != null) {
                if(aliasHashMap.containsKey(column.getTable().getName().toLowerCase())) {
                    tableName = aliasHashMap.get(column.getTable().getName().toLowerCase());
                    for(int i = 0; i < schema.length; i++) {
                        if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                            if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                                columnIndexes.add(i);
                                if(!projectionObjects.contains(schema[i].getWholeColumnName())){
                                    String c = schema[i].getColumnName();
                                    String m = tableName + "." + c;
                                    projectionObjects.add(m);
                                }
                                break;
                            }
                        }
                    }
                } else {

                }
            } else {
                for(int i = 0; i < schema.length; i++) {
                    if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                        columnIndexes.add(i);
                        if(!projectionObjects.contains(schema[i].getColumnName())){
                            String c = schema[i].getColumnName();
                            FromItem fromItem = plainSelect.getFromItem();
                            String t = "";
                            if(fromItem instanceof Table){
                                t = ((Table) fromItem).getWholeTableName();
                            }
                            String m = t + "." + c;
                            projectionObjects.add(m);
                        }
                        break;
                    }
                }

            }

        }
        else if(expression instanceof Function){
            if(projectionFlag){
                //CHECK if output is correct
                projectionObjects.add(expression.toString());
                Boolean isAsc = false;
                if(plainSelect.getOrderByElements()!=null){
                    isAsc = plainSelect.getOrderByElements().get(0).isAsc();
                    orderByExp = plainSelect.getOrderByElements().get(0).getExpression();
                }
                Function function = (Function) expression;
                String orderExpName = "";
                if(selectExpressionItem.getAlias()!=null){
                    projectionAliasName = selectExpressionItem.getAlias();
                    if(orderByExp instanceof Column){
                        orderExpName = ((Column) orderByExp).getColumnName();
                        if(!orderObject.contains(expression.toString())){
                            orderObject.add(expression.toString());
                        }
                        if(orderExpName.equals(projectionAliasName)){
                            OrderEvaluator orderEvaluator = new OrderEvaluator(function, groupByMap,
                                    aliasHashMap, schema, tupleList, isAsc, projectionAliasName,
                                    createTableMap, databaseMap);
                            projectionVisitorOutList = orderEvaluator.execute();
                            projectionSchema = orderEvaluator.projectionSchema;
                            groupByMap = orderEvaluator.groupByMap;
                            columnIndexes.add(projectionSchema.length - 1);
                        }
                    }
                    else{
                        System.out.println("ERROR ProjectinVisitor: orderByExp not a column");
                    }
                }
                else{
                    System.out.println("ERROR ProjectionVisitor: Function without alias");
                }
            }
        }
    }
}