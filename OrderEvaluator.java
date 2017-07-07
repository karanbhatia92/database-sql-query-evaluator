import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Mugdha on 7/4/2017.
 */
public class OrderEvaluator {
    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    ArrayList<PrimitiveValue[]> outputTupleList; // output of group or having or selection operation
    HashMap<String, String> aliasHashMap;
    Column[] schema;
    Function function;
    Boolean isAsc;

    public OrderEvaluator(Function function, HashMap groupByMap, HashMap aliasHashMap,
                          Column[] schema, ArrayList outputTupleList, Boolean isAsc){
        this.function = function;
        this.groupByMap = groupByMap;
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;
        this.outputTupleList = outputTupleList;
        this.isAsc = isAsc;
    }

    public ArrayList execute(){
        String funcitonName = function.getName();
        Boolean allColumns = function.isAllColumns();
        int columnIndex = 0;
        Expression columnExpression;
        String tableName;

        if(!allColumns){
            columnExpression = function.getParameters().getExpressions().get(0);
            if(columnExpression instanceof Column){
                Column column = (Column)columnExpression;
                if(column.getTable().getName().toLowerCase() != null) {
                    if(aliasHashMap.containsKey(column.getTable().getName().toLowerCase())) {
                        tableName = aliasHashMap.get(column.getTable().getName().toLowerCase());
                        for(int i = 0; i < schema.length; i++) {
                            if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                                if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                                    columnIndex = i;
                                    break;
                                }
                            }
                        }
                    } else {
                        System.out.println("ERROR in OrderEvaluator: alias not present in aliasHashMap");
                    }

                }
                else {
                    for(int i = 0; i < schema.length; i++) {
                        if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                            columnIndex = i;
                            break;
                        }
                    }

                }
            }

            Set keySet = groupByMap.keySet();
            Iterator itr = keySet.iterator();

        }
        return outputTupleList;
    }
}
