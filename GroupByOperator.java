import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Created by Mugdha on 7/2/2017.
 */
public class GroupByOperator {

    HashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> groupByMap = new HashMap<>();
    ArrayList<PrimitiveValue[]> groupByOutput = new ArrayList<>();
    HashMap<String, String> aliasHashMap;
    List<Column> groupByColumns;
    Column[] schema;
    String tableName;
    String aliasName;
    Integer columnIndex;

    public GroupByOperator(Column[] schema, List<Column> groupByColumns, HashMap<String, String> aliasHashMap){
        this.groupByColumns = groupByColumns;
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;

        Column groupByColumn = groupByColumns.get(0);
        if(groupByColumn.getTable().getName()!=null) {
            aliasName = groupByColumn.getTable().getName().toLowerCase();
            tableName = aliasHashMap.get(aliasName);
            for(int i = 0; i<schema.length; i++){
                if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                    if(schema[i].getColumnName().toLowerCase().equals(groupByColumn.getColumnName().toLowerCase())) {
                        columnIndex = i;
                        break;
                    }
                }
            }
        }
        else{
            for(int i = 0; i<schema.length; i++){
                if(schema[i].getColumnName().toLowerCase().equals(groupByColumn.getColumnName().toLowerCase())){
                    columnIndex = i;
                    break;
                }
            }
        }
    }

    public void groupTuples(PrimitiveValue[] tuple){
        if(groupByMap.containsKey(tuple[columnIndex])){
                ArrayList<PrimitiveValue[]> groupByArrayList = groupByMap.get(tuple[columnIndex]);
                groupByArrayList.add(tuple);
                groupByMap.replace(tuple[columnIndex],groupByArrayList);
            }
        else {
            ArrayList<PrimitiveValue[]> groupByArrayList = new ArrayList<>();
            groupByArrayList.add(tuple);
            groupByMap.put(tuple[columnIndex], groupByArrayList);

        }
    }

    public ArrayList getGroupByOutput(){
        Set keySet = groupByMap.keySet();
        Iterator itr = keySet.iterator();

        while (itr.hasNext()) {
            PrimitiveValue key = (PrimitiveValue) itr.next();
            ArrayList arraylist = groupByMap.get(key);
            for(int i =0; i< arraylist.size(); i++){
                PrimitiveValue[] tuple = (PrimitiveValue[]) arraylist.get(i);
                groupByOutput.add(tuple);
            }
        }
        return groupByOutput;
    }

}
