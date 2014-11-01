package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.PeekingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class WFSResultSet extends OGCResultSet {
	
	public static final String FID_NAME = "FID";
	
	private SimpleFeatureCollection features;
	private SimpleFeatureIterator it;

	public WFSResultSet(SimpleFeatureCollection features, ColumnGrouping colGroups) {
		this.features = features;
		this.it = features.features();
		this.columns = colGroups;
	}

	@Override
	protected TableRow makeNextRow() {
		TableRow nextRow = null;
		if (this.it.hasNext()) {
			SimpleFeature next = this.it.next();
			Map<Column, String> feature = new HashMap<>();
			for (Column col : columns) {
				String attribute;
				if (FID_NAME.equals(col.getName())) {
					attribute = "" + parseFID(next.getID());
				} else {
					attribute = "" + next.getAttribute(col.getName());
				}
				feature.put(col, attribute);
			}
			
			nextRow = new TableRow(columns, feature);
		}
		return nextRow;
	}

	@Override
	public void close() throws SQLException {
		IOUtils.closeQuietly(this.it);
		super.close();
	}

	private static Pattern fidPattern = Pattern.compile(".*\\.(\\d+)");
	private static int parseFID(String fid) {
		int result = -1;
		Matcher fidMatcher = fidPattern.matcher(fid);
		if (fidMatcher.matches()) {
			result = Integer.parseInt(fidMatcher.group(1));
		}
		return result;
	}

}
