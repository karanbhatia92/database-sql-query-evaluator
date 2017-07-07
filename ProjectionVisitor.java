import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;

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

    public ProjectionVisitor(Column[] schema, HashMap<String, String> aliasHashMap,
                             Boolean projectionFlag, PlainSelect plainSelect,
                             ArrayList tupleList, HashMap groupByMap) {
        columnIndexes = new ArrayList<>();
        this.schema = schema;
        this.aliasHashMap = aliasHashMap;
        this.projectionFlag = projectionFlag;
        this.plainSelect = plainSelect;
        this.tupleList = tupleList;
        this.groupByMap = groupByMap;
        this.projectionSchema = null;
    }
    public void visit(AllColumns allColumns) {
        for(int i = 0; i < schema.length; i++) {
            columnIndexes.add(i);
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
                }
            }
        }
    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        String tableName;
        String projectionAliasName = "";
        Expression expression = selectExpressionItem.getExpression();
        Expression orderByExp = plainSelect.getOrderByElements().get(0).getExpression();
        if(expression instanceof Column) {
            Column column = (Column)expression;
            if(column.getTable().getName() != null) {
                if(aliasHashMap.containsKey(column.getTable().getName().toLowerCase())) {
                    tableName = aliasHashMap.get(column.getTable().getName().toLowerCase());
                    for(int i = 0; i < schema.length; i++) {
                        if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                            if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                                columnIndexes.add(i);
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
                        break;
                    }
                }

            }

        }
        else if(expression instanceof Function){
            if(!(projectionFlag)){ // !projectionFlag indicates aggregation operation in select
                Function function = (Function) expression;
                String orderExpName = "";
                Boolean isAsc = false;
                if(selectExpressionItem.getAlias()!=null){
                    projectionAliasName = selectExpressionItem.getAlias();
                    if(orderByExp instanceof Column){
                        orderExpName = ((Column) orderByExp).getColumnName();
                        if(orderExpName.equals(projectionAliasName)){
                            OrderEvaluator orderEvaluator = new OrderEvaluator(function, groupByMap,
                                    aliasHashMap, schema, tupleList, isAsc);
                            projectionVisitorOutList = orderEvaluator.execute();
                            projectionSchema = orderEvaluator.projectionSchema;
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
