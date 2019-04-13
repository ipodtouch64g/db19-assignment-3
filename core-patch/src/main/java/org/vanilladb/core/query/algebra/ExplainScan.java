package org.vanilladb.core.query.algebra;

import java.util.Collection;

import org.vanilladb.core.sql.Constant;
import static org.vanilladb.core.sql.Type.VARCHAR;


public class ExplainScan implements Scan {
	private Scan s;
	private Collection<String> fieldList;
	private String planStr;

	public ExplainScan(Scan s, Collection<String> fieldList, String str) {
		this.s = s;
		this.fieldList = fieldList;
		this.planStr = str;
	}
	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName)) {
			if(fldName.equals("query-plan")) {
				Constant explain = Constant.newInstance(VARCHAR, planStr.getBytes());
				return explain;
			}
			else return s.getVal(fldName);
			}
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		return s.next();
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public boolean hasField(String fldName) {
		return fieldList.contains(fldName);
	}

}