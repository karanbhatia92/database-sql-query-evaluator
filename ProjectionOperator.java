import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Karan on 6/28/2017.
 */
public class ProjectionOperator{

    ArrayList<PrimitiveValue[]> outputList;
    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap;
    ArrayList<PrimitiveValue[]> tupleList;
    List<SelectItem> selectItemList;
    Column[] schema;
    HashMap<String, String> aliasHashMap;
    Boolean projectionFlag;
    PlainSelect plainSelect;
    Column[] newSchema;
    Column[] projectionSchema;
    public ProjectionOperator(ArrayList tupleList, List<SelectItem> selectItemList, Column[] schema,
                              HashMap<String, String> aliasHashMap, Boolean projectionFlag,
                              PlainSelect plainSelect, HashMap groupByMap) {
        this.tupleList = tupleList;
        this.selectItemList = selectItemList;
        this.schema = schema;
        this.aliasHashMap = aliasHashMap;
        this.projectionFlag = projectionFlag;
        this.plainSelect = plainSelect;
        this.groupByMap = groupByMap;
        this.outputList = new ArrayList<>();
    }

    public ArrayList<PrimitiveValue[]> getProjectedOutput() {

            ProjectionVisitor projectionVisitor = new ProjectionVisitor(schema, aliasHashMap,
                                                   projectionFlag, plainSelect, tupleList, groupByMap);
            int projectionIndex = 0;
            if(!projectionFlag){
                for(int i = 0; i<selectItemList.size(); i++){
                    if(selectItemList.get(i) instanceof Function){
                        projectionIndex = i;
                    }
                }
                selectItemList.get(projectionIndex).accept(projectionVisitor);
                tupleList = projectionVisitor.projectionVisitorOutList;
                projectionSchema = new Column[schema.length + 1];
                projectionSchema = projectionVisitor.projectionSchema;
            }
            else{
                projectionSchema = new Column[schema.length];
                projectionSchema = Arrays.copyOf(schema,schema.length);
            }
            for(int i = 0; i<selectItemList.size(); i++) {
                if(i != projectionIndex){
                    selectItemList.get(i).accept(projectionVisitor);
                }
            }
            PrimitiveValue[] tuple = new PrimitiveValue[projectionVisitor.columnIndexes.size()];

            newSchema = new Column[tuple.length];
            for (int i = 0; i < tuple.length; i++) {
                newSchema[i] = projectionSchema[projectionVisitor.columnIndexes.get(i)];
            }

            for(PrimitiveValue[] primitiveValues : tupleList){
                for (int i = 0; i < tuple.length; i++) {
                    tuple[i] = primitiveValues[projectionVisitor.columnIndexes.get(i)];
                }
                outputList.add(tuple);
            }

        return outputList;
    }
}
