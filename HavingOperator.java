import com.sun.org.apache.xpath.internal.operations.Bool;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.*;

/**
 * Created by Mugdha on 7/2/2017.
 */
public class HavingOperator {

    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    ArrayList<PrimitiveValue[]> outputTupleList;
    HashMap<String,String> aliasHashMap;
    ArrayList<PrimitiveValue[]> havingOutput;
    private Column[] schema;
    Expression condition;
    PlainSelect plainSelect;
    Boolean projectionFlag;

    public HavingOperator(Expression condition, HashMap groupByMap, HashMap aliasHashMap,
                          Column[] schema, PlainSelect plainSelect, Boolean projectionFlag,
                          ArrayList<PrimitiveValue[]> outputTupleList){
        this.condition = condition;
        this.groupByMap = groupByMap;
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;
        this.plainSelect = plainSelect;
        this.projectionFlag = projectionFlag;
        this.outputTupleList = outputTupleList;
        havingOutput = new ArrayList<>();
    }

    public void filterGroupedEle(){
        String functionName = "";
        String operation = "";
        long constantValue = 0;
        double aggResult = 0;
        int columnIndex = 0;

        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();
        ArrayList<PrimitiveValue> keyToRemove = new ArrayList();
        Boolean allColumns = false;
        Expression leftExpression = null;
        Expression rightExpression = null;
        Expression columnExpression;

        if(condition instanceof GreaterThan) {
            if(projectionFlag){
                leftExpression = plainSelect.getOrderByElements().get(0).getExpression();
            }
            else{
                leftExpression = ((GreaterThan) condition).getLeftExpression();
            }
            rightExpression = ((GreaterThan) condition).getRightExpression();
            operation = "GreaterThan";
        }
        else if (condition instanceof GreaterThanEquals){
            if(projectionFlag){
                leftExpression = plainSelect.getOrderByElements().get(0).getExpression();
            }
            else{
                leftExpression = ((GreaterThanEquals) condition).getLeftExpression();
            }
            rightExpression = ((GreaterThanEquals) condition).getRightExpression();
            operation = "GreaterThanEquals";
        }
        else if (condition instanceof MinorThan){
            if(projectionFlag){
                leftExpression = plainSelect.getOrderByElements().get(0).getExpression();
            }
            else{
                leftExpression = ((MinorThan) condition).getLeftExpression();
            }
            rightExpression = ((MinorThan) condition).getRightExpression();
            operation = "MinorThan";
        }
        else if (condition instanceof MinorThanEquals){
            if(projectionFlag){
                leftExpression = plainSelect.getOrderByElements().get(0).getExpression();
            }
            else{
                leftExpression = ((MinorThanEquals) condition).getLeftExpression();
            }
            rightExpression = ((MinorThanEquals) condition).getRightExpression();
            operation = "MinorThanEquals";
        }
        else if (condition instanceof EqualsTo){
            if(projectionFlag){
                leftExpression = plainSelect.getOrderByElements().get(0).getExpression();
            }
            else{
                leftExpression = ((EqualsTo) condition).getLeftExpression();
            }
            rightExpression = ((EqualsTo) condition).getRightExpression();
            operation = "EqualsTo";
        }
        else {
            System.out.println("ERROR in HavingOperator: expression type not handled");
        }

        if(leftExpression instanceof Function){
            functionName = ((Function) leftExpression).getName();
            allColumns = ((Function) leftExpression).isAllColumns();
            if(!allColumns) {
                columnExpression = ((Function) leftExpression).getParameters().getExpressions().get(0);
                columnIndex = this.getColumnIndex(columnExpression);
            }
        }
        else if (leftExpression instanceof Column){
            columnIndex = this.getColumnIndex(leftExpression);
        }

        if(rightExpression instanceof LongValue){
            constantValue = ((LongValue) rightExpression).getValue();
        }
        else{
            // CHECK FOR 11th QUERY
            System.out.println("Right expression not a constant");
        }

        if(projectionFlag){
/*
            for(int i = 0; i < outputTupleList.size(); i++){
                PrimitiveValue count =  outputTupleList[columnIndex];
                switch (operation){
                    case "GreaterThan":
                        if(aggResult <= constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "GreaterThanEquals":
                        if(aggResult < constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "MinorThan":
                        if(aggResult >= constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "MinorThanEquals":
                        if(aggResult > constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "EqualsTo":
                        if(aggResult != constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    default:
                        System.out.println("ERROR in HavingOperatior: Switch case expression not supported");
                }

            }
*/
        }

        else{
            while(itr.hasNext()){
                PrimitiveValue key = (PrimitiveValue) itr.next();
                ArrayList arraylist = groupByMap.get(key);
                if(!allColumns){
                    Double count = 0.0, sum = 0.0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                    for(int i = 0; i < arraylist.size(); i++){
                        PrimitiveValue[] primitiveValues = (PrimitiveValue[]) arraylist.get(i);
                        try{
                            sum = sum + primitiveValues[columnIndex].toDouble();
                            if(primitiveValues[columnIndex].toDouble() < min){
                                min = primitiveValues[columnIndex].toDouble();
                            }
                            if(primitiveValues[columnIndex].toDouble() > max){
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
                    if(functionName.length() > 1){
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
                        aggResult = sum;
                    }
                }
                else{
                    //System.out.println("allColumns are present only count is supported");
                    aggResult = arraylist.size();
                }

                switch (operation){
                    case "GreaterThan":
                        if(aggResult <= constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "GreaterThanEquals":
                        if(aggResult < constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "MinorThan":
                        if(aggResult >= constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "MinorThanEquals":
                        if(aggResult > constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    case "EqualsTo":
                        if(aggResult != constantValue){
                            keyToRemove.add(key);
                        }
                        break;
                    default:
                        System.out.println("ERROR in HavingOperatior: Switch case expression not supported");
                }

            }
        }

        for(int i = 0; i < keyToRemove.size(); i++){
            PrimitiveValue key = keyToRemove.get(i);
            groupByMap.remove(key);
        }

    }

    public int getColumnIndex(Expression leftExpression){
        int columnIndex = 0;
        String tableName = "";

        if(leftExpression instanceof Column){
            Column column = (Column)leftExpression;
            if(column.getTable().getName() != null) {
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
                    System.out.println("ERROR in HavingOperator: alias not present in aliasHashMap");
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
        return columnIndex;
    }

    public ArrayList getHavingOutput(){

        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();
        while (itr.hasNext()) {
            PrimitiveValue key = (PrimitiveValue) itr.next();
            ArrayList arraylist = groupByMap.get(key);
            PrimitiveValue[] tuple = (PrimitiveValue[]) arraylist.get(0);
            havingOutput.add(tuple);
        }
        return havingOutput;
    }

    public HashMap getMap(){
        return groupByMap;
    }

}
