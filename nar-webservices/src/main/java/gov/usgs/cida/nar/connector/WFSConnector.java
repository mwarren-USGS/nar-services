package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.resultset.WFSResultSet;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.wfs.HttpComponentsWFSClient;
import gov.usgs.cida.wfs.WFSClientInterface;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class WFSConnector implements IConnector {
	
	private static final Logger log = LoggerFactory.getLogger(WFSConnector.class);
	
	public static final String WFS_SITE_FID_COL_NAME = "FID";
	public static final String WFS_SITE_ID_COL_NAME = "qw_id";
	public static final String WFS_STA_NAME_COL_NAME = "qw_name";
	public static final String WFS_FLOW_ID_COL_NAME = "flow_id";
	public static final String WFS_FLOW_NAME_COL_NAME = "flow_name";
	public static final String WFS_DRAINAGE_AREA_COL_NAME = "da_mi2";
	public static final String WFS_LAT_COL_NAME = "qw_lat";
	public static final String WFS_LONG_COL_NAME = "qw_long";
	public static final String WFS_SITE_TYPE_COL_NAME = "site_type";
	public static final String WFS_STATE_COL_NAME = "state";
	public static final String WFS_MS_SITE_COL_NAME = "mssite";
	
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
		Column primaryKey = new SimpleColumn(WFS_SITE_FID_COL_NAME);
		List<Column> allColumns = new LinkedList<>();
		allColumns.add(primaryKey);
		allColumns.add(new SimpleColumn(WFS_SITE_ID_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_STA_NAME_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_FLOW_ID_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_FLOW_NAME_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_DRAINAGE_AREA_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_LAT_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_LONG_COL_NAME));
		allColumns.add(new SimpleColumn(WFS_SITE_TYPE_COL_NAME));
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
			SimpleFeatureCollection features = client.getFeatureCollection(this.typeName, this.filter);
			if(features != null) {
				wfsResultSet = new WFSResultSet(features, getExpectedColumns());
			} else {
				return new StringTableResultSet(getExpectedColumns());
			}
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
