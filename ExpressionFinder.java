import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.HashMap;

class ExpressionFinder implements ExpressionVisitor {


	ExpressionFinder(Expression expression) {
		expression.accept(this);
	}

	public void solve(Expression e) {
		e.accept(this);
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
		System.out.println("InsideLongValueExpression");
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
		System.out.println("InsideMultiplicationExpression");
	}
	public void visit(Subtraction subtraction) {
		System.out.println("InsideSubtractionExpression");
	}
	public void visit(AndExpression andExpression) {
		System.out.println("InsideANDExpression");
		Expression leftExpression = andExpression.getLeftExpression();
		System.out.println("VisitingLeftANDExpression");
		leftExpression.accept(this);
		Expression rightExpression = andExpression.getRightExpression();
		System.out.println("VisitingRightANDExpression");
		rightExpression.accept(this);
	}
	public void visit(OrExpression orExpression) {
		System.out.println("InsideORExpression");
	}
	public void visit(Between between) {
		System.out.println("InsideBetweenExpression");
	}

	public void visit(EqualsTo equalsTo) {
/*		System.out.println("InsideEqualsToExpression");

		Expression leftExpression = equalsTo.getLeftExpression();
		Expression rightExpression = equalsTo.getRightExpression();
		if(leftExpression instanceof Column && rightExpression instanceof Column) {
			System.out.println("Found Join");
			System.out.println(((Column)leftExpression).getTable().getName());
			System.out.println(((Column)rightExpression).getTable().getName());

			Column col1 = (Column)leftExpression;
			Column col2 = (Column)rightExpression;

			String tableName1 = col1.getTable().getName();
			String tableName2 = col2.getTable().getName();

			if(!(aliasTableMap.get(tableName1) == null)) {
				tableName1 = aliasTableMap.get(tableName1);
			}

			if(!(aliasTableMap.get(tableName2) == null)) {
				tableName2 = aliasTableMap.get(tableName2);
			}

			ScanOperator leftScanner = new ScanOperator(tableName1, tableMap);
			ScanOperator rightScanner = new ScanOperator(tableName2, tableMap);

			PrimitiveValue[] reader1;
			PrimitiveValue[] reader2;

			while((reader1 = leftScanner.readOneTuple()) != null) {
				rightScanner.reset();
				while((reader2 = rightScanner.readOneTuple()) != null) {

					Eval eval = new Eval(){
						public PrimitiveValue eval(Column c) {
							String colName = c.getColumnName();
							String tableName = c.getTable().getName();
							if(!(aliasTableMap.get(tableName) == null)) {
								tableName = aliasTableMap.get(tableName);
							}
							System.out.println("Column: " + colName +" AND Table: " + tableName);
							PrimitiveValue ret = null;
							if (colName.equals(col1.getColumnName())) {
								//ret = reader1[schemas.get(tableName).get(colName).colId];
							}
							if (colName.equals(col2.getColumnName())) {
								//ret = reader2[schemas.get(tableName).get(colName).colId];
							}
							//PrimitiveValue ret = new DoubleValue(7.0);
							return ret;
						}
					};

					//PrimitiveValue result = eval.eval(equalsTo);

					*//*if(result.toBool()) {
						System.out.println("Result: " + result);
						System.out.println("Row : " + reader2);
					}else {

					}*//*
				}
			}

			//PrimitiveValue result = eval.eval(equalsTo);
		}else if(leftExpression instanceof Column && rightExpression instanceof PrimitiveValue) {
			System.out.println("Found Comparison condition");
		}*/
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
		System.out.println("InsideTableColumnExpression");
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