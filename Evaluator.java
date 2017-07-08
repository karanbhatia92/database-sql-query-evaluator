import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Mugdha on 7/1/2017.
 */
public class Evaluator extends Eval {

    PrimitiveValue[] tuple;
    Column[] schema;
    HashMap<String, String> aliasHashMap;
    HashMap<String, CreateTable> createTableMap;
    HashMap<String, Integer> databaseMap;
    HashMap<String, PrimitiveValue> subSelectHash = null;

    public void setVariables(PrimitiveValue[] tuple, Column[] schema, HashMap<String, String> aliasHashMap,
                             HashMap<String, CreateTable> createTableMap, HashMap<String, Integer> databaseMap) {
        this.schema = schema;
        this.tuple = tuple;
        this.aliasHashMap = aliasHashMap;
        this.createTableMap = createTableMap;
        this.databaseMap = databaseMap;
    }

    public PrimitiveValue eval(Column c) throws SQLException {
        String colName = c.getColumnName().toLowerCase();
        String tableName = c.getTable().getName();
        int index = -1;
        if(tableName == null) {
            for(int i = 0; i < schema.length; i++) {
                if(schema[i].getColumnName().toLowerCase().equals(colName)) {
                    index = i;
                    break;
                }
            }
        } else {
            tableName = tableName.toLowerCase();
            String fullTableName = aliasHashMap.get(tableName);
            for(int i = 0; i < schema.length; i++) {
                if(schema[i].getColumnName().toLowerCase().equals(c.getColumnName().toLowerCase())) {
                    if(schema[i].getTable().getName().toLowerCase().equals(fullTableName)) {
                        index = i;
                    }
                }
            }


        }
        return tuple[index];
    }

    public PrimitiveType escalateNumeric(PrimitiveType lhs, PrimitiveType rhs)
            throws SQLException
    {
        if((lhs == PrimitiveType.DATE)||(rhs == PrimitiveType.DATE)) {
            return PrimitiveType.DATE;
        }
        if(  (assertNumeric(lhs) == PrimitiveType.DOUBLE)
                ||(assertNumeric(rhs) == PrimitiveType.DOUBLE)){
            return PrimitiveType.DOUBLE;
        } else {
            return PrimitiveType.LONG;
        }
    }

    public PrimitiveValue cmp(BinaryExpression e, CmpOp op)
            throws SQLException
    {
        try {
            PrimitiveValue lhs = eval(e.getLeftExpression());
            PrimitiveValue rhs = eval(e.getRightExpression());
            if(lhs == null || rhs == null) return null;
            boolean ret;

            switch(escalateNumeric(getPrimitiveType(lhs), getPrimitiveType(rhs))){
                case DOUBLE:
                    ret = op.op(lhs.toDouble(), rhs.toDouble());
                    break;
                case LONG:
                    ret = op.op(lhs.toLong(), rhs.toLong());
                    break;
                case DATE: {
                    String rhs1 = rhs.toRawString();
                    Date d = null;
                    try{
                        d = new SimpleDateFormat("yyyy-MM-dd").parse(rhs1);
                    }
                    catch (ParseException p){
                        p.printStackTrace();
                    }



                    DateValue dlhs = (DateValue)lhs;
                    //drhs = (DateValue)d;
                    ret = op.op(
                            dlhs.getYear()*10000+
                                    dlhs.getMonth()*100+
                                    dlhs.getDate(),
                            d.getYear()*10000+
                                    d.getMonth()*100+
                                    d.getDate()
                    );
                }
                break;
                default:
                    throw new SQLException("Invalid PrimitiveType escalation");
            }
            return ret ? BooleanValue.TRUE : BooleanValue.FALSE;
        } catch(PrimitiveValue.InvalidPrimitive ex) {
            throw new SQLException("Invalid leaf value", ex);
        }
    }

    public PrimitiveValue eval(InExpression in) throws SQLException {

        BooleanValue booleanValue = BooleanValue.FALSE;
        Expression leftExpression = in.getLeftExpression();
        Column c = (Column)leftExpression;
        String colName = c.getColumnName().toLowerCase();
        String tableName = c.getTable().getName();

        int index = -1;

        if(tableName == null) {
            for(int i = 0; i < schema.length; i++) {
                if(schema[i].getColumnName().toLowerCase().equals(colName)) {
                    index = i;
                    break;
                }
            }
        } else {
            tableName = tableName.toLowerCase();
            String fullTableName = aliasHashMap.get(tableName);
            for(int i = 0; i < schema.length; i++) {
                if(schema[i].getColumnName().toLowerCase().equals(c.getColumnName().toLowerCase())) {
                    if(schema[i].getTable().getName().toLowerCase().equals(fullTableName)) {
                        index = i;
                    }
                }
            }

        }
        ItemsList itemsList = in.getItemsList();
        Column[] colSchema = null;
        if(itemsList instanceof ExpressionList) {
            List<Expression> inExpressionList = ((ExpressionList) itemsList).getExpressions();
            Expression exp = inExpressionList.get(0);
            if(exp instanceof LongValue){
                if(tuple[index] instanceof LongValue) {
                    if(tuple[index].equals(exp)) {
                        booleanValue = BooleanValue.TRUE;
                    }
                }else {
                    return booleanValue;
                }
            }else if(exp instanceof DoubleValue) {
                if(tuple[index] instanceof DoubleValue) {
                    if(tuple[index].equals(exp)) {
                        booleanValue = BooleanValue.TRUE;
                    }
                }else {
                    return booleanValue;
                }
            }else {
                if(tuple[index] instanceof StringValue || tuple[index] instanceof DateValue) {
                    if(tuple[index].equals(exp)) {
                        booleanValue = BooleanValue.TRUE;
                    }
                }else {
                    return booleanValue;
                }
            }
            for (int i = 1; i < inExpressionList.size(); i++) {
                if(tuple[index].equals(inExpressionList.get(i))) {
                    booleanValue = BooleanValue.TRUE;
                }
            }
        } else if (itemsList instanceof SubSelect) {
            if(subSelectHash == null) {
                subSelectHash = new HashMap<>();
                SelectBody selectBody = ((SubSelect) itemsList).getSelectBody();
                if (selectBody instanceof PlainSelect) {
                    String alias = "in";
                    PlainSelect plainSelect = (PlainSelect) selectBody;
                    SubselectEvaluator subselect = new SubselectEvaluator(
                            plainSelect, createTableMap, alias, databaseMap
                    );
                    subselect.execute();
                    colSchema = subselect.schema;
                    PrimitiveValue tuple[];
                    while ((tuple = subselect.readOneTuple()) != null) {

                        String key = tuple[0].toRawString();
                        if(!subSelectHash.containsKey(key)) {
                            subSelectHash.put(key, tuple[0]);
                        }
                    }
                    subselect.reset();
                }

            }
            if(subSelectHash.containsKey(tuple[index].toRawString())) {
                booleanValue = BooleanValue.TRUE;
            }
        }
        return booleanValue;
    }
}
