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
import java.util.HashSet;
import java.util.Iterator;

class ExpressionFinder implements ExpressionVisitor {

	HashSet<String> printWhere;
	HashSet<String> joinprint;
	HashSet<String> fromObjects;
	HashMap<String, String> aliasHashMap;

	public ExpressionFinder(HashSet<String> fromObjects, HashMap<String, String> aliasHashMap) {
		printWhere = new HashSet<>();
		joinprint = new HashSet<>();
		this.fromObjects = fromObjects;
		this.aliasHashMap = aliasHashMap;
	}
	public HashSet<String> solve(Expression e) {
		e.accept(this);
		return printWhere;
	}
	public void visit(NullValue nullValue) {
		//System.out.println("InsideNullValueExpression");
	}
	public void visit(Function function) {
		//System.out.println("InsideFunctionExpression");
	}
	public void visit(InverseExpression inverseExpression) {
		//System.out.println("InsideInverseExpression");
	}
	public void visit(JdbcParameter jdbcParameter) {
		//System.out.println("InsideJdbcParameterExpression");
	}
	public void visit(DoubleValue doubleValue) {
		//System.out.println("InsideDoubleValueExpression");
	}
	public void visit(LongValue longValue) {
		//System.out.println("InsideLongValueExpression");
	}
	public void visit(DateValue dateValue) {
		//System.out.println("InsideDateValueExpression");
	}
	public void visit(TimeValue timeValue) {
		//System.out.println("InsideTimeValueExpression");
	}
	public void visit(TimestampValue timestampValue) {
		//System.out.println("InsideTimeStampValueExpression");
	}
	public void visit(BooleanValue booleanValue) {
		//System.out.println("InsideBooleanValueExpression");
	}
	public void visit(StringValue stringValue) {
		//System.out.println("InsideStringValueExpression");
	}
	public void visit(Addition addition) {
		//System.out.println("InsideAdditionExpression");
	}
	public void visit(Division division) {
		//System.out.println("InsideDivisionExpression");
	}
	public void visit(Multiplication multiplication) {
		//System.out.println("InsideMultiplicationExpression");
	}
	public void visit(Subtraction subtraction) {
		//System.out.println("InsideSubtractionExpression");
	}
	public void visit(AndExpression andExpression) {
		//System.out.println("InsideANDExpression");
		Expression leftExpression = andExpression.getLeftExpression();
		//System.out.println("VisitingLeftANDExpression");
		leftExpression.accept(this);
		Expression rightExpression = andExpression.getRightExpression();
		//System.out.println("VisitingRightANDExpression");
		rightExpression.accept(this);
	}
	public void visit(OrExpression orExpression) {
		//System.out.println("InsideORExpression");
		Expression leftExpression = orExpression.getLeftExpression();
		//System.out.println("VisitingLeftANDExpression");
		leftExpression.accept(this);
		Expression rightExpression = orExpression.getRightExpression();
		//System.out.println("VisitingRightANDExpression");
		rightExpression.accept(this);
	}
	public void visit(Between between) {
		//System.out.println("InsideBetweenExpression");
	}
	public void visit(EqualsTo equalsTo) {
		//System.out.println("InsideEqualsToExpression");

		Expression leftExpression = equalsTo.getLeftExpression();
		Expression rightExpression = equalsTo.getRightExpression();

		if(leftExpression instanceof Column && rightExpression instanceof Column) {
			Column leftCol = (Column)leftExpression;
			String leftTableName = leftCol.getTable().getName().toLowerCase();
			String leftColName = leftCol.getColumnName().toLowerCase();
			Column rightCol = (Column)rightExpression;
			String rightTableName = rightCol.getTable().getName().toLowerCase();
			String rightColName = rightCol.getColumnName().toLowerCase();

			leftTableName = aliasHashMap.get(leftTableName);
			rightTableName = aliasHashMap.get(rightTableName);

			joinprint.add(leftTableName + "." + leftColName);
			joinprint.add(rightTableName + "." + rightColName);
		}else {
			Column leftCol = (Column)leftExpression;
			String leftTableName = leftCol.getTable().getName();
			String leftColName = leftCol.getColumnName().toLowerCase();

			if(leftTableName == null) {
				Iterator<String> table = fromObjects.iterator();
				leftTableName = table.next();

				printWhere.add(leftTableName + "." + leftColName);
			}else {
				leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
				printWhere.add(leftTableName + "." + leftColName);
			}
		}

	}
	public void visit(GreaterThan greaterThan) {
		//System.out.println("InsideGreaterThanExpression");

		Expression leftExpression = greaterThan.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();

		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}
	}
	public void visit(GreaterThanEquals greaterThanEquals) {
		//System.out.println("InsideGreaterThanEqualsExpression");

		Expression leftExpression = greaterThanEquals.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();

		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}
	}
	public void visit(InExpression inExpression) {
		//System.out.println("InsideInExpression");
		Expression leftExpression = inExpression.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();

		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}

		ItemsList itemsList = inExpression.getItemsList();
		if (itemsList instanceof SubSelect) {

		}
	}
	public void visit(IsNullExpression isNullExpression) {
		//System.out.println("InsideIsNullExpression");
	}
	public void visit(LikeExpression likeExpression) {
		//System.out.println("InsideLikeExpression");

		Expression leftExpression = likeExpression.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();


		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}
	}
	public void visit(MinorThan minorThan) {
		//System.out.println("InsideMinorThanExpression");

		Expression leftExpression = minorThan.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();

		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}
	}
	public void visit(MinorThanEquals minorThanEquals) {
		//System.out.println("InsideMinorThanEqualsExpression");

		Expression leftExpression = minorThanEquals.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();

		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}
	}
	public void visit(NotEqualsTo notEqualsTo) {
		//System.out.println("InsideNotEqualsToExpression");

		Expression leftExpression = notEqualsTo.getLeftExpression();
		Column leftCol = (Column)leftExpression;
		String leftTableName = leftCol.getTable().getName();
		String leftColName = leftCol.getColumnName().toLowerCase();

		if(leftTableName == null) {
			Iterator<String> table = fromObjects.iterator();
			leftTableName = table.next();

			printWhere.add(leftTableName + "." + leftColName);
		}else {
			leftTableName = aliasHashMap.get(leftTableName.toLowerCase());
			printWhere.add(leftTableName + "." + leftColName);
		}
	}
	public void visit(Column tableColumn) {
		//System.out.println("InsideTableColumnExpression");
	}
	public void visit(SubSelect subSelect) {
		System.out.println("InsideSubSelectExpression");
	}
	public void visit(CaseExpression caseExpression) {
		//System.out.println("InsideCaseExpression");
	}
	public void visit(WhenClause whenClause) {
		//System.out.println("InsideWhenClauseExpression");
	}
	public void visit(ExistsExpression existsExpression) {
		//System.out.println("InsideExistsExpression");
	}
	public void visit(AllComparisonExpression allComparisonExpression) {
		//System.out.println("InsideAllComparisonExpression");
	}
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		//System.out.println("InsideAnyComparisonExpression");
	}
	public void visit(Concat concat) {
		//System.out.println("InsideConcatExpression");
	}
	public void visit(Matches matches) {
		//System.out.println("InsideMatchesExpression");
	}
	public void visit(BitwiseAnd bitwiseAnd) {
		//System.out.println("InsideBitwiseAndExpression");
	}
	public void visit(BitwiseOr bitwiseOr) {
		//System.out.println("InsideBitwiseORExpression");
	}
	public void visit(BitwiseXor bitwiseXor) {
		//System.out.println("InsideBitwiseXORExpression");
	}
}