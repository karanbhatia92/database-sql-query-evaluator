import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

/**
 * Created by Karan on 6/28/2017.
 */
public class ProjectionOperator{

    ArrayList<PrimitiveValue[]> outputList;
    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    HashMap<String, CreateTable> createTableMap;
    ArrayList<PrimitiveValue[]> tupleList;
    List<SelectItem> selectItemList;
    Column[] schema;
    HashMap<String, String> aliasHashMap;
    Boolean projectionFlag;
    PlainSelect plainSelect;
    Column[] newSchema;
    Column[] projectionSchema;
    HashSet<String> projectionObjects;
    HashSet<String> orderObject;
    HashMap<String, Integer> databaseMap;

    public ProjectionOperator(ArrayList tupleList, List<SelectItem> selectItemList, Column[] schema,
                              HashMap<String, String> aliasHashMap, Boolean projectionFlag,
                              PlainSelect plainSelect, HashMap groupByMap, HashMap createTableMap,
                              HashMap<String, Integer> databaseMap) {
        this.tupleList = tupleList;
        this.selectItemList = selectItemList;
        this.schema = schema;
        this.databaseMap = databaseMap;
        this.aliasHashMap = aliasHashMap;
        this.projectionFlag = projectionFlag;
        this.plainSelect = plainSelect;
        this.groupByMap = groupByMap;
        this.createTableMap = createTableMap;
        this.outputList = new ArrayList<>();
        projectionObjects = new HashSet<>();
        orderObject = new HashSet<>();
    }

    public ArrayList<PrimitiveValue[]> getProjectedOutput() {

        ProjectionVisitor projectionVisitor = new ProjectionVisitor(schema, aliasHashMap, projectionFlag,
                                                plainSelect, tupleList, groupByMap,
                                                createTableMap, databaseMap);
        int projectionIndex = Integer.MIN_VALUE;
        if(projectionFlag){
            for(int i = 0; i<selectItemList.size(); i++){
                if(selectItemList.get(i) instanceof SelectExpressionItem){
                    Expression expression = ((SelectExpressionItem) selectItemList.get(i)).getExpression();
                    if(expression instanceof Function){
                        projectionIndex = i;
                    }
                }
            }
        }

        // Add columnIndex in general case
        projectionSchema = new Column[schema.length];
        projectionSchema = Arrays.copyOf(schema,schema.length);
        for (int i = 0; i < selectItemList.size(); i++) {
            if(i == projectionIndex){
                selectItemList.get(projectionIndex).accept(projectionVisitor);
                tupleList = projectionVisitor.projectionVisitorOutList;
                groupByMap = projectionVisitor.groupByMap;
                projectionSchema = new Column[schema.length + 1];
                projectionSchema = projectionVisitor.projectionSchema;
            }
            else {
                selectItemList.get(i).accept(projectionVisitor);
            }
        }

        newSchema = new Column[projectionVisitor.columnIndexes.size()];
        int schemaCount  = 0;
        for(PrimitiveValue[] primitiveValues : tupleList){
            PrimitiveValue[] tuple = new PrimitiveValue[projectionVisitor.columnIndexes.size()];
            for (int i = 0; i < tuple.length; i++) {
                tuple[i] = primitiveValues[projectionVisitor.columnIndexes.get(i)];
                if(schemaCount == 0){
                    newSchema[i] = projectionSchema[projectionVisitor.columnIndexes.get(i)];
                }
            }
            schemaCount = schemaCount + 1;
            outputList.add(tuple);
        }
        projectionObjects = projectionVisitor.projectionObjects;
        orderObject = projectionVisitor.orderObject;
        return outputList;
    }
}
