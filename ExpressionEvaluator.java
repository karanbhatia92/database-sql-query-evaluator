import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.HashMap;

class ExpressionEvaluator implements ExpressionVisitor {

    HashMap<String, String> aliasHashMap;
    Column[] schema;
    ArrayList<Integer> columnIndexList;
    ArrayList<Long> constantList;

    ExpressionEvaluator(HashMap aliasHashMap, Column[] schema) {
        this.columnIndexList = new ArrayList<>();
        this.constantList = new ArrayList<>();
        this.aliasHashMap = aliasHashMap;
        this.schema = schema;
    }

    public void solve(Expression expression) {
        expression.accept(this);
    }
    public void visit(NullValue nullValue) {
        System.out.println("InsideNullValueExpression");
    }
    public void visit(Function function) {
        System.out.println("InsideFunctionExpression");
    }
    public void visit(InverseExpression inverseExpression) {
        System.out.println("InsideInverseExpression");
    }
    public void visit(JdbcParameter jdbcParameter) {
        System.out.println("InsideJdbcParameterExpression");
    }
    public void visit(DoubleValue doubleValue) {
        System.out.println("InsideDoubleValueExpression");
    }
    public void visit(LongValue longValue) {
        constantList.add(longValue.getValue());
    }
    public void visit(DateValue dateValue) {
        System.out.println("InsideDateValueExpression");
    }
    public void visit(TimeValue timeValue) {
        System.out.println("InsideTimeValueExpression");
    }
    public void visit(TimestampValue timestampValue) {
        System.out.println("InsideTimeStampValueExpression");
    }
    public void visit(BooleanValue booleanValue) {
        System.out.println("InsideBooleanValueExpression");
    }
    public void visit(StringValue stringValue) {
        System.out.println("InsideStringValueExpression");
    }
    public void visit(Addition addition) {
        System.out.println("InsideAdditionExpression");
    }
    public void visit(Division division) {
        System.out.println("InsideDivisionExpression");
    }
    public void visit(Multiplication multiplication) {
        Expression leftExpression = multiplication.getLeftExpression();
        leftExpression.accept(this);
        Expression rightExpression = multiplication.getRightExpression();
        rightExpression.accept(this);
    }
    public void visit(Subtraction subtraction) {
        Expression leftExpression = subtraction.getLeftExpression();
        leftExpression.accept(this);
        Expression rightExpression = subtraction.getRightExpression();
        rightExpression.accept(this);
    }
    public void visit(AndExpression andExpression) { System.out.println("InsideANDExpression"); }
    public void visit(OrExpression orExpression) {
        System.out.println("InsideORExpression");
    }
    public void visit(Between between) {
        System.out.println("InsideBetweenExpression");
    }
    public void visit(EqualsTo equalsTo) {
    }
    public void visit(GreaterThan greaterThan) {
        System.out.println("InsideGreaterThanExpression");
    }
    public void visit(GreaterThanEquals greaterThanEquals) {
        System.out.println("InsideGreaterThanEqualsExpression");
    }
    public void visit(InExpression inExpression) {
        System.out.println("InsideInExpression");
    }
    public void visit(IsNullExpression isNullExpression) {
        System.out.println("InsideIsNullExpression");
    }
    public void visit(LikeExpression likeExpression) {
        System.out.println("InsideLikeExpression");
    }
    public void visit(MinorThan minorThan) {
        System.out.println("InsideMinorThanExpression");
    }
    public void visit(MinorThanEquals minorThanEquals) {
        System.out.println("InsideMinorThanEqualsExpression");
    }
    public void visit(NotEqualsTo notEqualsTo) {
        System.out.println("InsideNotEqualsToExpression");
    }
    public void visit(Column tableColumn) {

        Column column = tableColumn;
        String tableName;
        if( column.getTable().getWholeTableName()!= null) {
            if(aliasHashMap.containsKey(column.getTable().getName().toLowerCase())) {
                tableName = aliasHashMap.get(column.getTable().getName().toLowerCase());
                for(int i = 0; i < schema.length; i++) {
                    if(schema[i].getTable().getName().toLowerCase().equals(tableName)) {
                        if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                            columnIndexList.add(i);
                            break;
                        }
                    }
                }
            } else {
                System.out.println("ERROR in ExpressionEvaluator: alias not present in aliasHashMap");
            }
        }
        else {
            for(int i = 0; i < schema.length; i++) {
                if(schema[i].getColumnName().toLowerCase().equals(column.getColumnName().toLowerCase())) {
                    columnIndexList.add(i);
                    break;
                }
            }
        }

    }
    public void visit(SubSelect subSelect) {
        System.out.println("InsideSubSelectExpression");
    }
    public void visit(CaseExpression caseExpression) {
        System.out.println("InsideCaseExpression");
    }
    public void visit(WhenClause whenClause) {
        System.out.println("InsideWhenClauseExpression");
    }
    public void visit(ExistsExpression existsExpression) {
        System.out.println("InsideExistsExpression");
    }
    public void visit(AllComparisonExpression allComparisonExpression) {
        System.out.println("InsideAllComparisonExpression");
    }
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        System.out.println("InsideAnyComparisonExpression");
    }
    public void visit(Concat concat) {
        System.out.println("InsideConcatExpression");
    }
    public void visit(Matches matches) {
        System.out.println("InsideMatchesExpression");
    }
    public void visit(BitwiseAnd bitwiseAnd) {
        System.out.println("InsideBitwiseAndExpression");
    }
    public void visit(BitwiseOr bitwiseOr) {
        System.out.println("InsideBitwiseORExpression");
    }
    public void visit(BitwiseXor bitwiseXor) {
        System.out.println("InsideBitwiseXORExpression");
    }

}