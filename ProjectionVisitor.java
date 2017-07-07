import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Karan on 6/28/2017.
 */
public class ProjectionVisitor implements SelectItemVisitor {

    public Column[] schema = null;
    public ArrayList<Integer> columnIndexes;
    HashMap<String, String> aliasHashMap;

    public ProjectionVisitor(Column[] schema, HashMap<String, String> aliasHashMap) {
        columnIndexes = new ArrayList<>();
        this.schema = schema;
        this.aliasHashMap = aliasHashMap;
    }
    public void visit(AllColumns allColumns) {
        for(int i = 0; i < schema.length; i++) {
            columnIndexes.add(i);
        }
    }

    public void visit(AllTableColumns allTableColumns) {

        String aliasName = allTableColumns.getTable().getWholeTableName().toLowerCase();
        String tableName;
        if(aliasHashMap.containsKey(aliasName)){
            tableName = aliasHashMap.get(aliasName);
            for(int i = 0; i < schema.length; i++){
                if(schema[i].getTable().getWholeTableName().toLowerCase().equals(tableName)){
                    columnIndexes.add(i);
                }
            }
        }
    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        String tableName;
        selectExpressionItem.getAlias();
        Expression expression = selectExpressionItem.getExpression();
        if(expression instanceof Column) {
            Column column = (Column)expression;
            if(column.getTable().getName() != null) {
                if(aliasHashMap.containsKey(column.getTable().getName().toLowerCase())) {
                    tableName = aliasHashMap.get(column.getTable().getName().toLowerCase());
                    for(int i = 0; i < schema.length; i++) {
                        if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                            if(schema[i].getColumnName().equals(column.getColumnName())) {
                                columnIndexes.add(i);
                                break;
                            }
                        }
                    }
                } else {

                }
            } else {
                for(int i = 0; i < schema.length; i++) {
                    if(schema[i].getColumnName().equals(column.getColumnName())) {
                        columnIndexes.add(i);
                        break;
                    }
                }

            }

        }
    }

}
