import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by Mugdha on 7/3/2017.
 */
public class SubMain {
    public PlainSelect plainSelect;
    public HashMap<String, CreateTable> createTableMap;
    public Column[] schema;
    public Column[] newSchema;
    HashMap<String, Integer> databaseMap;
    HashSet<String> fromObjects;
    HashSet<String> projectionObjects;
    HashSet<String> groupObject;
    HashSet<String> orderObject;

    public SubMain(PlainSelect plainSelect, HashMap createTableMap, HashMap<String, Integer> databaseMap){
        this.plainSelect = plainSelect;
        this.createTableMap = createTableMap;
        this.databaseMap = databaseMap;
        projectionObjects = new HashSet<>();
        fromObjects = new HashSet<>();
        groupObject = new HashSet<>();
        orderObject = new HashSet<>();
    }
    public ArrayList execute(){

        HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap = new HashMap<>();
        ArrayList<PrimitiveValue[]> outputTupleList = new ArrayList<>();
        HashMap<String, Long> fileSizeMap = new HashMap<>();
        HashMap<String, String> aliasHashMap;
        HashMap<String, Operator> operatorMap;
        List<Join> joinList;
        Operator oper = null;

        FromScanner fromscan = new FromScanner(createTableMap, databaseMap);
        plainSelect.getFromItem().accept(fromscan);

        if(plainSelect.getJoins() != null) {
            joinList = plainSelect.getJoins();
            for(Join join : joinList) {
                join.getRightItem().accept(fromscan);
            }
        } else {

        }
        aliasHashMap = fromscan.aliasHasMap;
        operatorMap = fromscan.operatorMap;
        fileSizeMap = fromscan.fileSizeMap;
        groupObject = fromscan.groupObject;
        fromObjects = fromscan.fromObjects;
        orderObject = fromscan.orderObject;

        schema = new Column[fromscan.schemaList.size()];
        schema = fromscan.schemaList.toArray(schema);
        Column[] projectedSchema = new Column[fromscan.schemaList.size()+1];
        createTableMap = fromscan.createTableMap;
        if(plainSelect.getWhere() != null) {
            oper = new SelectionOperator(
                    databaseMap,
                    operatorMap,
                    schema,
                    plainSelect.getWhere(),
                    aliasHashMap,
                    createTableMap,
                    fileSizeMap
            );
        } else {
            for (String key : operatorMap.keySet()) {
                oper = operatorMap.get(key);
            }
        }

        //group by

        if(plainSelect.getGroupByColumnReferences()!=null){
            List<Column> groupByColumns = plainSelect.getGroupByColumnReferences();
            if(!groupObject.contains(groupByColumns.get(0).getWholeColumnName())){
                groupObject.add(groupByColumns.get(0).getWholeColumnName());
            }

            PrimitiveValue[] tuple = oper.readOneTuple();
            GroupByOperator groupByOperator = new GroupByOperator(schema, groupByColumns, aliasHashMap);
            while(tuple!=null){
                groupByOperator.groupTuples(tuple);
                tuple = oper.readOneTuple();
            }
            groupByMap = groupByOperator.groupByMap;
            outputTupleList = groupByOperator.getGroupByOutput();
        }
        else{
            PrimitiveValue[] tuple = oper.readOneTuple();
            while(tuple!=null){
                outputTupleList.add(tuple);
                tuple = oper.readOneTuple();
            }
        }
        // get projection condition
        Boolean projectionFlag = false;
        ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) plainSelect.getSelectItems();
        for(SelectItem selectItem : selectItems){
            if(selectItem instanceof SelectExpressionItem){
                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                if(expression instanceof Function){
                    projectionFlag = true;
                }
            }
        }
        if(projectionFlag && (plainSelect.getHaving()!=null)){
            //Projection
            List<SelectItem> selectItemList = plainSelect.getSelectItems();
            ProjectionOperator projectionOperator = new ProjectionOperator(
                    outputTupleList, selectItemList, schema, aliasHashMap,
                    projectionFlag, plainSelect, groupByMap, createTableMap
            );
            outputTupleList = projectionOperator.getProjectedOutput();
            groupByMap = projectionOperator.groupByMap;
            newSchema = projectionOperator.newSchema;
            projectionObjects = projectionOperator.projectionObjects;
            HashSet<String> tempOrder = projectionOperator.orderObject;
            if(!tempOrder.isEmpty()){
                for(String str : tempOrder){
                    if(!orderObject.contains(str)){
                        orderObject.add(str);
                    }
                }
            }

            // Having
            Expression condition = plainSelect.getHaving();
            HavingOperator havingOperator = new HavingOperator(condition, groupByMap, aliasHashMap,
                    newSchema, plainSelect, projectionFlag, outputTupleList);
            havingOperator.filterGroupedEle();
            outputTupleList = havingOperator.getHavingOutput();

        }
        else{
            // Having
            if(plainSelect.getHaving()!=null){
                Expression condition = plainSelect.getHaving();
                HavingOperator havingOperator = new HavingOperator(condition, groupByMap, aliasHashMap,
                                                    schema, plainSelect, projectionFlag,outputTupleList);
                havingOperator.filterGroupedEle();
                outputTupleList = havingOperator.getHavingOutput();
                groupByMap = havingOperator.getMap();
            }
            //Order By
            if(plainSelect.getOrderByElements()!=null){
                List<OrderByElement> orderByList = plainSelect.getOrderByElements();
                OrderByOperator orderByOperator = new OrderByOperator(
                        groupByMap, aliasHashMap, schema, plainSelect, outputTupleList
                );
                orderByOperator.orderTuples(orderByList);
                outputTupleList = orderByOperator.getOrderByOutput();
                groupByMap = orderByOperator.groupByMap;
                HashSet<String> temp = orderByOperator.orderObject;
                if(!temp.isEmpty()){
                    for(String str : temp){
                        orderObject.add(str);
                    }
                }
            }
            //Projection
            List<SelectItem> selectItemList = plainSelect.getSelectItems();
            ProjectionOperator projectionOperator = new ProjectionOperator(
                    outputTupleList, selectItemList, schema, aliasHashMap,
                    projectionFlag, plainSelect, groupByMap, createTableMap
            );
            outputTupleList = projectionOperator.getProjectedOutput();
            newSchema = projectionOperator.newSchema;
            projectionObjects = projectionOperator.projectionObjects;
            HashSet<String> tempOrder = projectionOperator.orderObject;
            if(!tempOrder.isEmpty()){
                for(String str : tempOrder){
                    if(!orderObject.contains(str)){
                        orderObject.add(str);
                    }
                }
            }
        }

        //Distinct
        Distinct distinct = plainSelect.getDistinct();
        if(distinct != null){
            DistinctOperator distinctOperator = new DistinctOperator(outputTupleList);
            outputTupleList = distinctOperator.execute();
        }

        if(plainSelect.getLimit()!=null){
            Long limit = plainSelect.getLimit().getRowCount();
            int i = limit.intValue();
            while(outputTupleList.size() > limit){
                outputTupleList.remove(i);
            }
        }

        return outputTupleList;
    }
}
