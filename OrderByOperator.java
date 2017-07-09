import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

/**
 * Created by Mugdha on 7/2/2017.
 */
public class OrderByOperator {

    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    ArrayList<PrimitiveValue[]> outputTupleList; // output of group or having or selection operation
    ArrayList<PrimitiveValue[]> orderByOutput;
    HashMap<String, String> aliasHashMap;
    Column[] schema;
    PlainSelect plainSelect;
    String groupByColumnName;
    String orderByColumnName;
    HashSet<String> orderObject;

    public OrderByOperator(
            HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap,
            HashMap<String, String> aliasHashMap,
            Column[] schema,
            PlainSelect plainSelect,
            ArrayList outputTupleList
    ){
        this.groupByMap = groupByMap;
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;
        this.plainSelect = plainSelect;
        this.outputTupleList = outputTupleList;
        orderByOutput = new ArrayList<>();
        groupByColumnName = "";
        orderByColumnName = "";
        orderObject = new HashSet<>();
    }

    public void orderTuples(List<OrderByElement> orderByList){
        Boolean isAsc = orderByList.get(0).isAsc();
        Integer columnIndexOrder = 0;
        String tableName;
        String aliasName;
        Expression orderByExp = orderByList.get(0).getExpression();

        if(orderByExp instanceof Column) {
            Column column = (Column)orderByExp;
            orderByColumnName = column.getColumnName().toLowerCase();
            if(column.getTable().getName() != null) {
                if(!orderObject.contains(column.getWholeColumnName())){
                    orderObject.add(column.getWholeColumnName());
                }
                aliasName = column.getTable().getName().toLowerCase();
                if(aliasHashMap.containsKey(aliasName)){
                    tableName = aliasHashMap.get(aliasName);
                    for(int i = 0; i < schema.length; i++) {
                        if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                            if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                                columnIndexOrder = i;
                                break;
                            }
                        }
                    }
                }
                else{
                    System.out.println("ERROR in OrderByOperator: alias not present in aliasHashMap");
                }
            } else {
                for(int i = 0; i < schema.length; i++) {
                    if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                        if(!orderObject.contains(column.getWholeColumnName())){
                            orderObject.add(column.getWholeColumnName());
                        }
                        columnIndexOrder = i;
                        break;
                    }
                }

            }
        }

        else{
            System.out.println("ERROR in OrderByOperator: Expression not handled");
        }


        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();
        if(plainSelect.getGroupByColumnReferences()==null){
            OrderComparator orderComparator = new OrderComparator(
                    aliasHashMap, outputTupleList.get(0), columnIndexOrder, schema, isAsc
            );
            Collections.sort(outputTupleList,orderComparator);
        }

        if(plainSelect.getGroupByColumnReferences()!=null){
            groupByColumnName = plainSelect.getGroupByColumnReferences().get(0).getColumnName().toLowerCase();
            if(groupByColumnName.equals(orderByColumnName)){
                while(itr.hasNext()){
                    PrimitiveValue key = (PrimitiveValue) itr.next();
                    ArrayList<PrimitiveValue[]> arrayList = groupByMap.get(key);
                    orderByOutput.add(arrayList.get(0));
                    ArrayList<PrimitiveValue[]> tempList = new ArrayList<>();
                    tempList.add(arrayList.get(0));
                    groupByMap.put(key,tempList);
                }
                OrderComparator orderComparator = new OrderComparator(
                        aliasHashMap, orderByOutput.get(0), columnIndexOrder, schema, isAsc
                );
                Collections.sort(orderByOutput,orderComparator);
            }
            else{
                while (itr.hasNext()){
                    PrimitiveValue key = (PrimitiveValue) itr.next();
                    ArrayList<PrimitiveValue[]> arrayList = groupByMap.get(key);
                    OrderComparator orderComparator = new OrderComparator(
                            aliasHashMap, arrayList.get(0), columnIndexOrder, schema, isAsc
                    );
                    Collections.sort(arrayList,orderComparator);
                    groupByMap.replace(key,arrayList);
                }
            }
        }

    }

    public ArrayList getOrderByOutput(){

        if(plainSelect.getGroupByColumnReferences()==null){
            return outputTupleList;
        }
        else if(plainSelect.getGroupByColumnReferences()!=null){
            if(groupByColumnName.equals(orderByColumnName)){
                return orderByOutput;
            }
            else{
                Set keySet = groupByMap.keySet();
                Iterator itr = keySet.iterator();
                while (itr.hasNext()) {
                    PrimitiveValue key = (PrimitiveValue) itr.next();
                    ArrayList arraylist = groupByMap.get(key);
                    //CHECK all tuples should be outputted
                    for(int i =0; i< arraylist.size(); i++){
                        PrimitiveValue[] tuple = (PrimitiveValue[]) arraylist.get(i);
                        orderByOutput.add(tuple);
                    }
                }
                return orderByOutput;
            }
        }

        return outputTupleList;
    }


}
