import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Karan on 6/28/2017.
 */
public class ProjectionOperator{

    ArrayList<PrimitiveValue[]> outputList = new ArrayList<>();
    ArrayList<PrimitiveValue[]> tupleList;
    List<SelectItem> selectItemList;
    Column[] schema;
    HashMap<String, String> aliasHashMap;
    Column[] newSchema;
    public ProjectionOperator(ArrayList tupleList, List<SelectItem> selectItemList,
                              Column[] schema, HashMap<String, String> aliasHashMap) {
        this.tupleList = tupleList;
        this.selectItemList = selectItemList;
        this.schema = schema;
        this.aliasHashMap = aliasHashMap;
    }

    public ArrayList<PrimitiveValue[]> getProjectedOutput() {
        for(int j = 0; j < tupleList.size(); j++){
            PrimitiveValue[] inputTuple = tupleList.get(j);
            if (inputTuple == null) {
                continue;
            }
            ProjectionVisitor projectionVisitor = new ProjectionVisitor(schema, aliasHashMap);
            for(SelectItem selectItem : selectItemList) {
                selectItem.accept(projectionVisitor);
            }
            PrimitiveValue[] tuple = new PrimitiveValue[projectionVisitor.columnIndexes.size()];
            if(j == 0) {
                newSchema = new Column[tuple.length];
                for (int i = 0; i < tuple.length; i++) {
                    newSchema[i] = schema[projectionVisitor.columnIndexes.get(i)];
                }
            }
            for (int i = 0; i < tuple.length; i++) {
                tuple[i] = inputTuple[projectionVisitor.columnIndexes.get(i)];
            }
            outputList.add(tuple);
        }
        return outputList;
    }
}
