import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by Mugdha on 7/5/2017.
 */
public class DistinctOperator {
    ArrayList<PrimitiveValue[]> outputTupleList = new ArrayList<>();
    ArrayList<PrimitiveValue[]> distinctOutput = new ArrayList<>();

    public DistinctOperator(ArrayList outputTupleList){
        this.outputTupleList = outputTupleList;
    }

    public ArrayList<PrimitiveValue[]> execute(){
        HashSet<String> distinctKeySet = new HashSet<>();
        ArrayList<Integer> listIndex = new ArrayList<>();

        for(int i = 0; i < outputTupleList.size(); i++){
            StringBuilder stringBuilder = new StringBuilder();
            PrimitiveValue[] primitiveValues = outputTupleList.get(i);
            for(PrimitiveValue primitiveValue : primitiveValues){
                stringBuilder.append(primitiveValue.toRawString());
            }
            if(!(distinctKeySet.contains(stringBuilder.toString()))){
                distinctKeySet.add(stringBuilder.toString());
                listIndex.add(i);
            }
        }
        for(int i = 0; i < listIndex.size(); i++){
            distinctOutput.add(outputTupleList.get(listIndex.get(i)));
        }

        return distinctOutput;
    }
}
