import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by Mugdha on 7/1/2017.
 */
public class Evaluator extends Eval {

    PrimitiveValue[] tuple;
    Column[] schema;
    HashMap<String, String> aliasHashMap;

    public Evaluator(PrimitiveValue[] tuple, Column[] schema, HashMap<String, String> aliasHashMap){
        this.schema = schema;
        this.tuple = tuple;
        this.aliasHashMap = aliasHashMap;
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
}
