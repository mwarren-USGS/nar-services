package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.resultset.WFSResultSet;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.wfs.HttpComponentsWFSClient;
import gov.usgs.cida.wfs.WFSClientInterface;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class WFSConnector implements IConnector {
	
	private static final Logger log = LoggerFactory.getLogger(WFSConnector.class);
	
	private ColumnGrouping cg;
	private WFSClientInterface client;
	private String typeName;
	private Filter filter;
	
	public WFSConnector (String wfsEndpoint, String typeName, Filter filter) {
		this.client = new HttpComponentsWFSClient();
		try {
			this.client.setupDatastoreFromEndpoint(wfsEndpoint);
		}
		catch (IOException ex) {
			log.error("Could not set up wfs connector", ex);
		}
		this.cg = makeColumnGrouping();
		this.typeName = typeName;
		this.filter = filter;
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
		WFSResultSet wfsResultSet = null;
		try {
			wfsResultSet = new WFSResultSet(client.getFeatureCollection(this.typeName, this.filter), getExpectedColumns());
		}
		catch (IOException ex) {
			log.error("Unable to get wfs result set from source", ex);
		}
		return wfsResultSet;
	}

	@Override
	public IParser getParser() {
		return null;
	}

	@Override
	public boolean isValidInput() {
		return true;
	}

	@Override
	public boolean isReady() {
		return true;
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.cg;
	}

}
