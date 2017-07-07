import net.sf.jsqlparser.expression.PrimitiveValue;

/**
 * Created by Karan on 6/26/2017.
 */
public interface Operator {
    public PrimitiveValue[] readOneTuple();
    public void reset();
}
