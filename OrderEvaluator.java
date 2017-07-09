import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by Mugdha on 7/4/2017.
 */
public class OrderEvaluator {
    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    ArrayList<PrimitiveValue[]> outputTupleList; // output of group or having or selection operation
    ArrayList<PrimitiveValue[]> orderEvalOutput;
    HashMap<String, String> aliasHashMap;
    HashMap<String, Integer> databaseMap;

    HashMap<String, CreateTable> createTableMap;
    String projectionAliasName = "";
    Column[] schema;
    public Column[] projectionSchema;
    Function function;
    Boolean isAsc;

    public OrderEvaluator(Function function, HashMap groupByMap, HashMap aliasHashMap,
                          Column[] schema, ArrayList outputTupleList, Boolean isAsc,
                          String projectionAliasName,HashMap<String, CreateTable> createTableMap,
                          HashMap<String, Integer> databaseMap){
        this.function = function;
        this.groupByMap = groupByMap;
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;
        this.outputTupleList = outputTupleList;
        this.isAsc = isAsc;
        this.orderEvalOutput = new ArrayList<>();
        this.projectionAliasName = projectionAliasName;
        this.createTableMap = createTableMap;
        this.databaseMap = databaseMap;

    }

    public ArrayList execute(){
        HashMap<Double, ArrayList<PrimitiveValue[]>> aggregateMap = new HashMap<>();
        String functionName = function.getName();
        Boolean allColumns = function.isAllColumns();
        int schemaSize = schema.length + 1;
        Double aggResult = 0.0;

        projectionSchema = new Column[schemaSize];
        for(int i = 0; i < schema.length; i++){
            projectionSchema[i] = schema[i];
        }
        projectionSchema[schemaSize-1] = new Column(null,projectionAliasName);

        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();
        Expression columnExpression = function.getParameters().getExpressions().get(0);

        while(itr.hasNext()){
            PrimitiveValue key = (PrimitiveValue) itr.next();
            ArrayList arraylist = groupByMap.get(key);
            if(!allColumns){
                Double count = 0.0, sum = 0.0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                for(int i = 0; i < arraylist.size(); i++){
                    Evaluator evaluator = new Evaluator();
                    evaluator.setVariables(((PrimitiveValue[]) arraylist.get(i)), schema,
                            aliasHashMap, createTableMap, databaseMap);
                    try{
                        PrimitiveValue result = evaluator.eval(columnExpression);

                            sum = sum + result.toDouble();
                            if(result.toDouble() < min){
                                min = result.toDouble();
                            }
                            if(result.toDouble() > max){
                                max = result.toDouble();
                            }
                            count = count + 1;

                        if(result instanceof LongValue){

                        }
                        else if (result instanceof DoubleValue){

                        }
                    }
                    catch (SQLException e){
                        e.printStackTrace();
                    }
                }
                if(functionName.toLowerCase().equals("count")){
                    aggResult = count;
                }
                else if(functionName.toLowerCase().equals("avg")){
                    aggResult = sum/count;
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

            PrimitiveValue pv = new DoubleValue(aggResult.toString());
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
