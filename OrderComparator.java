import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Created by Mugdha on 7/2/2017.
 */
public class OrderComparator implements Comparator<PrimitiveValue[]> {
    HashMap<String, String> aliasHashMap = new HashMap<>();
    PrimitiveValue[] tuple;
    Integer columnIndex;
    Column[] schema;
    Boolean isAsc;

    public OrderComparator(
            HashMap<String, String> aliasHashMap,
            PrimitiveValue[] tuple,
            Integer columnIndex,
            Column[] schema,
            Boolean isAsc
    ){
        this.aliasHashMap = aliasHashMap;
        this.tuple = tuple;
        this.columnIndex = columnIndex;
        this.schema = schema;
        this.isAsc = isAsc;
    }

    @Override
    public int compare(PrimitiveValue[] pv1, PrimitiveValue[] pv2) {
        Evaluator eval = new Evaluator(tuple,schema,aliasHashMap);

        if(isAsc){
            GreaterThan cmp = new GreaterThan();
            if(pv1[columnIndex] instanceof DateValue){
                cmp.setRightExpression(new DateValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new DateValue(pv2[columnIndex].toString()));
            }
            else if(pv1[columnIndex] instanceof LongValue){
                cmp.setRightExpression(new LongValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new LongValue(pv2[columnIndex].toString()));
            }
            else if(pv1[columnIndex] instanceof DoubleValue){
                cmp.setRightExpression(new DoubleValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new DoubleValue(pv2[columnIndex].toString()));
            }
            else if(pv1[columnIndex] instanceof StringValue){
                cmp.setRightExpression(new StringValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new StringValue(pv2[columnIndex].toString()));
            }

            try{
                PrimitiveValue result = eval.eval(cmp);
                if(!result.toBool()){
                    return 1;
                } else {
                    return -1;
                }
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        else{
            MinorThan cmp = new MinorThan();

            if(pv1[columnIndex] instanceof DateValue){
                cmp.setRightExpression(new DateValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new DateValue(pv2[columnIndex].toString()));
            }
            else if(pv1[columnIndex] instanceof LongValue){
                cmp.setRightExpression(new LongValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new LongValue(pv2[columnIndex].toString()));
            }
            else if(pv1[columnIndex] instanceof DoubleValue){
                cmp.setRightExpression(new DoubleValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new DoubleValue(pv2[columnIndex].toString()));
            }
            else if(pv1[columnIndex] instanceof StringValue){
                cmp.setRightExpression(new StringValue(pv1[columnIndex].toString()));
                cmp.setLeftExpression(new StringValue(pv2[columnIndex].toString()));
            }

            try{
                PrimitiveValue result = eval.eval(cmp);
                if(!result.toBool()){
                    return 1;
                } else {
                    return -1;
                }
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return -1;
    }
}
