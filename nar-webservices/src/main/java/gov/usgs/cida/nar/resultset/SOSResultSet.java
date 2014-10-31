package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.ObservationCollection;
import java.sql.SQLException;
import org.apache.commons.io.IOUtils;


/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSResultSet extends OGCResultSet {
	
	private ObservationCollection observations;

	public SOSResultSet(ObservationCollection observations, ColumnGrouping colGroups) {
		this.observations = observations;
		this.columns = colGroups;
	}

	@Override
	public void close() throws SQLException {
		IOUtils.closeQuietly(observations);
		super.close();
	}

	@Override
	protected TableRow makeNextRow() {
//		TableRow row = null;
//		if (observations.hasNext()) {
//			Observation next = observations.next();
//			Map<Column, String> ob = new HashMap<>();
//			for (Column col : columns) {
//				String attribute = null;
//				if (col.getName())
//				String attribute = next.get(col.getName());
//				ob.put(col, attribute);
//			}
//			
//			nextRow = new TableRow(columns, ob);
//			}
//		}
		throw new UnsupportedOperationException("Not yet implemented");
	}
	
	
}
