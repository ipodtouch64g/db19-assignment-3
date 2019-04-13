package org.vanilladb.core.query.algebra;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;


public class ExplainPlan implements Plan {

	private Plan p;
	private Schema schema = new Schema();

	public ExplainPlan(Plan p) {
		this.p = p;
	}

	@Override
	public Scan open() {
		Scan s = p.open();
		String str = explain(0, "");
		return new ExplainScan(s, schema.fields(), str);
	}

	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) p.histogram().recordsOutput();
	}

	@Override
	public String explain(int tab, String plan) {
		plan += "\n";
		plan = p.explain(0, plan);
		plan +="Actual #recs: " + recordsOutput() + "\n";

		Type temp = Type.newInstance(java.sql.Types.VARCHAR, 500);
		this.schema.addField("query-plan", temp);
		return plan;
	}

}