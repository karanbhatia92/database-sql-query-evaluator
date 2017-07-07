import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Karan on 6/27/2017.
 */
public class FromScanner implements FromItemVisitor {

    HashMap<String, CreateTable> createTableMap;
    HashMap<String, String> aliasHasMap;
    HashMap<String, Operator> operatorMap;
    ArrayList<Column> schemaList;
    public Operator source = null;

    public FromScanner(HashMap<String, CreateTable> createTableMap) {
        this.createTableMap = createTableMap;
        aliasHasMap = new HashMap<>();
        operatorMap = new HashMap<>();
        schemaList = new ArrayList<>();
    }
    public void visit(SubJoin subjoin) {

    }

    public void visit(SubSelect subSelect) {
        String alias = "";
        Column[] tempSchema;
        if(subSelect.getAlias()!=null){
            alias = subSelect.getAlias();
            aliasHasMap.put(alias,"fromTable");
        }
        else {
            System.out.println("WARNING: FromScanner SubSelect: alias added as FT");
            alias = "FT";
        }
        SelectBody selectBody = subSelect.getSelectBody();
        if(selectBody instanceof PlainSelect){
            PlainSelect plainSelect = (PlainSelect) selectBody;
            SubselectEvaluator subselectEvaluator = new SubselectEvaluator(
                    plainSelect, createTableMap, alias
            );
            subselectEvaluator.execute();
            tempSchema = subselectEvaluator.schema;
            for(int i = 0; i < tempSchema.length; i++){
                schemaList.add(tempSchema[i]);
            }
            this.createTableMap = subselectEvaluator.createTableMap;
            operatorMap.put("fromTable",subselectEvaluator);
        }
        else{
            System.out.println("ERROR: FromScanner : Union not handled in subSelect");
        }
    }

    public void visit(Table table) {
        CreateTable ct = createTableMap.get(table.getName().toLowerCase());
        if(table.getAlias() != null) {
            aliasHasMap.put(table.getAlias().toLowerCase(), table.getName().toLowerCase());
        }
        List cols = ct.getColumnDefinitions();

        for(int i = 0; i < cols.size(); i++) {
            ColumnDefinition col = (ColumnDefinition)cols.get(i);
            schemaList.add(new Column(table, col.getColumnName().toLowerCase()));
        }
        source = new ScanOperator(
                new File(table.getName().toLowerCase() + ".csv"), ct);
        operatorMap.put(table.getName().toLowerCase(), source);
    }
}
