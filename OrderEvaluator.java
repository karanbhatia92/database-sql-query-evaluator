import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
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
    String projectionAliasName = "";
    Column[] schema;
    public Column[] projectionSchema;
    Function function;
    Boolean isAsc;

    public OrderEvaluator(Function function, HashMap groupByMap, HashMap aliasHashMap,
                          Column[] schema, ArrayList outputTupleList, Boolean isAsc,
                          String projectionAliasName){
        this.function = function;
        this.groupByMap = groupByMap;
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;
        this.outputTupleList = outputTupleList;
        this.isAsc = isAsc;
        this.orderEvalOutput = new ArrayList<>();
        this.projectionAliasName = projectionAliasName;
    }

    public ArrayList execute(){
        String functionName = function.getName();
        Boolean allColumns = function.isAllColumns();
        Expression columnExpression;
        int schemaSize = schema.length + 1;
        ArrayList<Integer> columnIndexList = new ArrayList<>();
        ArrayList<Long> constantList = new ArrayList<>();
        projectionSchema = new Column[schemaSize];
        for(int i = 0; i < schema.length; i++){
            projectionSchema[i] = schema[i];
        }
        projectionSchema[schema.length] = new Column(null,projectionAliasName);

        if(!allColumns){
            columnExpression = function.getParameters().getExpressions().get(0);
            ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator(aliasHashMap, schema);
            expressionEvaluator.solve(columnExpression);
            columnIndexList = expressionEvaluator.columnIndexList;
            constantList = expressionEvaluator.constantList;
        }

        HashMap<Double, ArrayList<PrimitiveValue[]>> aggregateMap = new HashMap<>();
        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();
        Double aggResult = 0.0;

        while(itr.hasNext()){
            PrimitiveValue key = (PrimitiveValue) itr.next();
            ArrayList arraylist = groupByMap.get(key);
            if(!allColumns){
                Double count = 0.0, sum = 0.0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                if(columnIndexList.size() > 1){
                    for(int i = 0; i < arraylist.size(); i++){
                        PrimitiveValue[] primitiveValues = (PrimitiveValue[]) arraylist.get(i);
                        try{
                            Double diff = constantList.get(0).doubleValue() - primitiveValues[columnIndexList.get(1)].toDouble();
                            Double mult = primitiveValues[columnIndexList.get(0)].toDouble() * diff;
                            sum = sum + mult;
                        }
                        catch (PrimitiveValue.InvalidPrimitive e){
                            e.printStackTrace();
                        }
                    }
                }

                else if(columnIndexList.size() == 1){
                    int columnIndex = columnIndexList.get(0);
                    for(int i = 0; i < arraylist.size(); i++){
                        PrimitiveValue[] primitiveValues = (PrimitiveValue[]) arraylist.get(i);
                        try{
                            sum = sum + primitiveValues[columnIndex].toLong();
                            if(primitiveValues[columnIndex].toLong() < min){
                                min = primitiveValues[columnIndex].toDouble();
                            }
                            if(primitiveValues[columnIndex].toLong() > max){
                                max = primitiveValues[columnIndex].toDouble();
                            }
                            if(primitiveValues[columnIndex]!=null){
                                count = count + 1;
                            }
                        }
                        catch (PrimitiveValue.InvalidPrimitive e){
                            e.printStackTrace();
                        }
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
                Integer temp = arraylist.size();
                aggResult = temp.doubleValue();
            }

            PrimitiveValue pv;
            if(functionName.toLowerCase().equals("count")){
                pv = new LongValue(aggResult.toString());
            }
            else{
                pv = new DoubleValue(aggResult.toString());
            }
            PrimitiveValue[] tempPrimVals = Arrays.copyOf((PrimitiveValue[]) arraylist.get(0),schemaSize);
            tempPrimVals[schemaSize - 1] = pv;
            if(aggregateMap.containsKey(aggResult)){
                ArrayList<PrimitiveValue[]> primitiveValues = aggregateMap.get(aggResult);
                primitiveValues.add(tempPrimVals);
                aggregateMap.put(aggResult, primitiveValues);
            }
            else{
                ArrayList<PrimitiveValue[]> primitiveValues = new ArrayList<>();
                primitiveValues.add(tempPrimVals);
                aggregateMap.put(aggResult, primitiveValues);
            }
            ArrayList<PrimitiveValue[]> tempList = new ArrayList<>();
            tempList.add(tempPrimVals);
            groupByMap.replace(key,tempList);
        }
        Set<Double> aggValues = aggregateMap.keySet();
        Object[] aggValuesArray = aggValues.toArray();
        Arrays.sort(aggValuesArray);
        if(isAsc){
            for(int i = 0; i < aggValuesArray.length; i++){
                ArrayList<PrimitiveValue[]> primitiveValuesList = aggregateMap.get(aggValuesArray[i]);
                for(PrimitiveValue[] primitiveValues : primitiveValuesList){
                    orderEvalOutput.add(primitiveValues);
                }
            }
        }
        else{
            for(int i = (aggValuesArray.length - 1); i >= 0; i--){
                ArrayList<PrimitiveValue[]> primitiveValuesList = aggregateMap.get(aggValuesArray[i]);
                for(PrimitiveValue[] primitiveValues : primitiveValuesList){
                    orderEvalOutput.add(primitiveValues);
                }
            }
        }
        return orderEvalOutput;
    }
}
