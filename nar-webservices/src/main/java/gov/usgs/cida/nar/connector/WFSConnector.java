package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.parser.CSVDataParser;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.resultset.http.HttpResultSet;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class WFSConnector implements IConnector {
	
	private InputStream input;
	private ColumnGrouping cg;
	
	public WFSConnector (InputStream input) {
		this.input = input;
		this.cg = makeColumnGrouping();
	}
	
	private static ColumnGrouping makeColumnGrouping() {
		Column primaryKey = new SimpleColumn("FID");
		List<Column> allColumns = new LinkedList<>();
		allColumns.add(primaryKey);
		allColumns.add(new SimpleColumn("siteid"));
		allColumns.add(new SimpleColumn("staname"));
		allColumns.add(new SimpleColumn("latitude"));
		allColumns.add(new SimpleColumn("longitude"));
		allColumns.add(new SimpleColumn("sitetype"));
		return new ColumnGrouping(primaryKey, allColumns);
	}

	@Override
	public void addInput(ResultSet rs) {
		return;
	}

	@Override
	public String getStatement() {
		return null;
	}

	@Override
	public ResultSet getResultSet() {
		return new HttpResultSet(new StupidHttpEntity(input), getParser());
	}

	@Override
	public IParser getParser() {
		// TODO replace with proper WFS parser
		DateTimeFormatter fidFormatter = new DateTimeFormatterBuilder().appendLiteral("JD_NFSN_sites0914.").appendYear(1, 4).toFormatter();
		return new CSVDataParser(this.cg, "FID", new Column[] {
			this.cg.get(1), null, this.cg.get(2), this.cg.get(3), this.cg.get(4), this.cg.get(5), this.cg.get(6)
		}, this.cg.getPrimaryKey(), fidFormatter);
	}

	@Override
	public boolean isValidInput() {
		return true;
	}

	@Override
	public boolean isReady() {
		return (input != null);
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.cg;
	}

}
