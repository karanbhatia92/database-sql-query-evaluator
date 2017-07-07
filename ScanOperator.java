import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Karan on 6/26/2017.
 */
public class ScanOperator implements Operator {
    BufferedReader input;
    File f;
    CreateTable ct;

    public ScanOperator(File f, CreateTable ct) {
        this.f = f;
        this.ct = ct;
        reset();
    }

    public PrimitiveValue[] readOneTuple() {
        if (input == null) {
            return null;
        }
        String line = null;
        try {
            line = input.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line == null) {
            return null;
        }

        String[] cols = line.split("\\|");
        PrimitiveValue ret[] = new PrimitiveValue[cols.length];
        List<ColumnDefinition> columnDefinitionList = ct.getColumnDefinitions();
        for (int i = 0; i < cols.length; i++) {
            if(cols[i].equals("NULL")) {
                ret[i] = new NullValue();
                continue;
            }
            String colDataType = columnDefinitionList.get(i).getColDataType().getDataType();
            switch (colDataType) {
                case "STRING":
                case "VARCHAR":
                case "CHAR":
                    ret[i] = new StringValue(cols[i]);
                    break;
                case "INTEGER":
                    ret[i] = new LongValue(cols[i]);
                    break;
                case "DECIMAL":
                    ret[i] = new DoubleValue(cols[i]);
                    break;
                case "DATE":
                    ret[i] = new DateValue(cols[i]);
                    break;
                case "DOUBLE":
                    ret[i] = new DoubleValue(cols[i]);
                    break;
                default:
                    ret[i] = null;
            }
        }
        return ret;
    }

    public void reset() {
        try {
            input = new BufferedReader(new FileReader(f));
        } catch (IOException e) {
            e.printStackTrace();
            input = null;
        }
    }
}
