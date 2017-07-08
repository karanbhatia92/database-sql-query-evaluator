import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by Karan on 6/27/2017.
 */
public class FromScanner implements FromItemVisitor {

    HashMap<String, CreateTable> createTableMap;
    HashMap<String, String> aliasHasMap;
    HashMap<String, Operator> operatorMap;
    HashMap<String, Long> fileSizeMap;
    ArrayList<Column> schemaList;
    HashMap<String, Integer> databaseMap;
    public Operator source = null;
    HashSet<String> fromObjects;
    HashSet<String> groupObject;
    HashSet<String> orderObject;

    public FromScanner(HashMap<String, CreateTable> createTableMap, HashMap<String, Integer> databaseMap) {
        this.createTableMap = createTableMap;
        this.databaseMap = databaseMap;
        aliasHasMap = new HashMap<>();
        operatorMap = new HashMap<>();
        schemaList = new ArrayList<>();
        fileSizeMap = new HashMap<>();
        fromObjects = new HashSet<>();
        groupObject = new HashSet<>();
        orderObject = new HashSet<>();
    }
    public void visit(SubJoin subjoin) {

    }

    public void visit(SubSelect subSelect) {
        String alias = "";
        Column[] tempSchema;
        if(subSelect.getAlias()!=null){
            alias = subSelect.getAlias().toLowerCase();
            aliasHasMap.put(alias,"fromtable");
        }
        else {
            //System.out.println("WARNING: FromScanner SubSelect: alias added as FT");
            alias = "ft";
        }
        SelectBody selectBody = subSelect.getSelectBody();
        if(selectBody instanceof PlainSelect){
            PlainSelect plainSelect = (PlainSelect) selectBody;
            SubselectEvaluator subselectEvaluator = new SubselectEvaluator(
                    plainSelect, createTableMap, alias, databaseMap
            );
            subselectEvaluator.execute();
            tempSchema = subselectEvaluator.schema;
            for(int i = 0; i < tempSchema.length; i++){
                schemaList.add(tempSchema[i]);
            }
            this.createTableMap = subselectEvaluator.createTableMap;
            operatorMap.put("fromtable",subselectEvaluator);
            HashSet<String> tempfromObjects = subselectEvaluator.fromObjects;
            if(tempfromObjects != null){
                for(String str : tempfromObjects){
                    if(!fromObjects.contains(str)){
                        fromObjects.add(str);
                    }
                }
            }
            if(groupObject != null){
                groupObject = subselectEvaluator.groupObject;
            }
            if(orderObject != null){
                orderObject = subselectEvaluator.orderObject;
            }
        }
        else{
            System.out.println("ERROR: FromScanner : Union not handled in subSelect");
        }
    }

    public void visit(Table table) {

        if(!fromObjects.contains(table.getWholeTableName())){
            fromObjects.add(table.getWholeTableName());
        }
        CreateTable ct = createTableMap.get(table.getName().toLowerCase());
        if(table.getAlias() != null) {
            aliasHasMap.put(table.getAlias().toLowerCase(), table.getName().toLowerCase());
        }
        List cols = ct.getColumnDefinitions();

        for(int i = 0; i < cols.size(); i++) {
            ColumnDefinition col = (ColumnDefinition)cols.get(i);
            schemaList.add(new Column(table, col.getColumnName().toLowerCase()));
        }
        File file = new File(table.getName().toLowerCase() + ".csv");
        source = new ScanOperator(file, ct);
        operatorMap.put(table.getName().toLowerCase(), source);
        fileSizeMap.put(table.getName().toLowerCase(), file.length());
    }
}
