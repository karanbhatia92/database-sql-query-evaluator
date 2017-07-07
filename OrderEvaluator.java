import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Created by Mugdha on 7/4/2017.
 */
public class OrderEvaluator {
    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    ArrayList<PrimitiveValue[]> outputTupleList; // output of group or having or selection operation
    ArrayList<PrimitiveValue[]> orderEvalOutput;
    HashMap<String, String> aliasHashMap;
    Column[] schema;
    public Column[] projectionSchema;
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
        this.orderEvalOutput = new ArrayList<>();
    }

    public ArrayList execute(){
        String functionName = function.getName();
        Boolean allColumns = function.isAllColumns();
        int columnIndex = 0;
        Expression columnExpression;
        String tableName;
        int schemaSize = schema.length + 1;
        projectionSchema = new Column[schemaSize];
        projectionSchema[schemaSize - 1] = new Column(null,"aggresult");

        if(!allColumns){
            columnExpression = function.getParameters().getExpressions().get(0);
            if(columnExpression instanceof Column){
                Column column = (Column)columnExpression;
                //CHECK THIS CONDITION
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
            // CHECK expressions for 11th query
        }

        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();
        HashMap<Long, ArrayList<PrimitiveValue[]>> aggregateMap = new HashMap<>();
        long aggResult = 0;

        while(itr.hasNext()){
            PrimitiveValue key = (PrimitiveValue) itr.next();
            ArrayList arraylist = groupByMap.get(key);
            if(!allColumns){
                // handle double case
                long count = 0, sum = 0, min = Long.MAX_VALUE, max = Long.MIN_VALUE;
                for(int i = 0; i < arraylist.size(); i++){
                    PrimitiveValue[] primitiveValues = (PrimitiveValue[]) arraylist.get(i);
                    try{
                        sum = sum + primitiveValues[columnIndex].toLong();
                        if(primitiveValues[columnIndex].toLong() < min){
                            min = primitiveValues[columnIndex].toLong();
                        }
                        if(primitiveValues[columnIndex].toLong() > max){
                            max = primitiveValues[columnIndex].toLong();
                        }
                        if(primitiveValues[columnIndex]!=null){
                            count = count + 1;
                        }
                    }
                    catch (PrimitiveValue.InvalidPrimitive e){
                        e.printStackTrace();
                    }
                }
                if(functionName.toLowerCase().equals("count")){
                    aggResult = count;
                }
                else if(functionName.toLowerCase().equals("avg")){
                    aggResult = sum/arraylist.size();
                }
                else if(functionName.toLowerCase().equals("sum")){
                    aggResult = sum;
                }
                else if(functionName.toLowerCase().equals("max")){
                    aggResult = max;
                }
                else if(functionName.toLowerCase().equals("min")){
                    aggResult = min;
                }
            }
            else{
                aggResult = arraylist.size();
            }
            if(aggregateMap.containsKey(aggResult)){
                ArrayList<PrimitiveValue[]> primitiveValues = aggregateMap.get(aggResult);
                primitiveValues.add((PrimitiveValue[]) arraylist.get(0));
                aggregateMap.put(aggResult, primitiveValues);
            }
            else{
                ArrayList<PrimitiveValue[]> primitiveValues = new ArrayList<>();
                primitiveValues.add((PrimitiveValue[]) arraylist.get(0));
                aggregateMap.put(aggResult, primitiveValues);
            }
        }
        Set<Long> aggValues = aggregateMap.keySet();
        Object[] aggValuesArray = aggValues.toArray();
        Arrays.sort(aggValuesArray);
        if(isAsc){
            for(int i = 0; i < aggValuesArray.length; i++){
                ArrayList<PrimitiveValue[]> primitiveValuesList = aggregateMap.get(aggValuesArray[i]);
                for(PrimitiveValue[] primitiveValues : primitiveValuesList){
                    PrimitiveValue[] tempPrimVals = Arrays.copyOf(primitiveValues,schemaSize);
                    tempPrimVals[schemaSize - 1] = (PrimitiveValue) aggValuesArray[i];
                    orderEvalOutput.add(tempPrimVals);
                }
            }
        }
        else{
            for(int i = (aggValuesArray.length - 1); i >= 0; i--){
                ArrayList<PrimitiveValue[]> primitiveValuesList = aggregateMap.get(aggValuesArray[i]);
                for(PrimitiveValue[] primitiveValues : primitiveValuesList){
                    PrimitiveValue[] tempPrimVals = Arrays.copyOf(primitiveValues,schemaSize);
                    tempPrimVals[schemaSize - 1] = (PrimitiveValue) aggValuesArray[i];
                    orderEvalOutput.add(tempPrimVals);
                }
            }
        }
        return orderEvalOutput;
    }
}
