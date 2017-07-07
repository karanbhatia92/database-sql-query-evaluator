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
    Boolean projectionFlag = false;

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
    }

    public void orderTuples(List<OrderByElement> orderByList){
        Boolean isAsc = orderByList.get(0).isAsc();
        Integer columnIndexOrder = 0;
        String tableName;
        String aliasName;
        Expression orderByExp = orderByList.get(0).getExpression();

        if(orderByExp instanceof Column) {
            projectionFlag = true;
            Column column = (Column)orderByExp;
            orderByColumnName = column.getColumnName();
            if(column.getTable().getName() != null) {
                aliasName = column.getTable().getName();
                if(aliasHashMap.containsKey(aliasName)){
                    tableName = aliasHashMap.get(aliasName);
                    for(int i = 0; i < schema.length; i++) {
                        if(schema[i].getTable().getName().equals(tableName)) {
                            if(schema[i].getColumnName().equals(column.getColumnName())) {
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
                    if(schema[i].getColumnName().equals(column.getColumnName())) {
                        columnIndexOrder = i;
                        break;
                    }
                }

            }
        }
        else if(orderByExp instanceof StringValue){
            String orderExpName = ((StringValue) orderByExp).getNotExcapedValue();
            String projectionAliasName = "";
            ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) plainSelect.getSelectItems();
            for(SelectItem selectItem : selectItems) {
                if (selectItem instanceof SelectExpressionItem) {
                    Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                    if (expression instanceof Function) {
                        Function function = (Function) expression;
                        if(((SelectExpressionItem) selectItem).getAlias()!=null){
                            projectionAliasName = ((SelectExpressionItem) selectItem).getAlias();
                        }
                        if(orderExpName.equals(projectionAliasName)){
                            // order evaluatior class returns final arraylist and group by map
                            OrderEvaluator orderEvaluator = new OrderEvaluator(function, groupByMap,
                                                aliasHashMap, schema, outputTupleList, isAsc);
                            orderByList = orderEvaluator.execute();
                        }
                    }
                }
            }
        }
        else{
            System.out.println("ERROR in OrderByOperator: Expression not handled");
        }

        if(projectionFlag){
            Set keySet = groupByMap.keySet();
            Iterator itr = keySet.iterator();
            if(plainSelect.getGroupByColumnReferences()==null){
                OrderComparator orderComparator = new OrderComparator(
                        aliasHashMap, outputTupleList.get(0), columnIndexOrder, schema, isAsc
                );
                Collections.sort(outputTupleList,orderComparator);
            }

            if(plainSelect.getGroupByColumnReferences()!=null){
                groupByColumnName = plainSelect.getGroupByColumnReferences().get(0).getColumnName();
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
    }

    public ArrayList getOrderByOutput(){
        if(projectionFlag){
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
                        for(int i =0; i< arraylist.size(); i++){
                            PrimitiveValue[] tuple = (PrimitiveValue[]) arraylist.get(0);
                            orderByOutput.add(tuple);
                        }
                    }
                    return orderByOutput;
                }
            }
        }

        return orderByOutput;
    }


}
