package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.Observation;
import gov.usgs.cida.sos.ObservationCollection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
		TableRow row = null;
		if (observations.hasNext()) {
			Observation next = observations.next();
			Map<Column, String> ob = new HashMap<>();
			for (Column col : columns) {
				String attribute = null;
				if (col.equals(SOSConnector.SOS_DATE_COL_NAME)) {
					attribute = next.time().toString();
				}
				if (col.equals(SOSConnector.SOS_PROCEDURE_COL_NAME)) {
					attribute = next.metadata().procedure();
				}
				if (col.equals(SOSConnector.SOS_CONSTITUENT_COL_NAME)) {
					attribute = next.metadata().observedProperty();
				}
				if (col.equals(SOSConnector.SOS_SITE_COL_NAME)) {
					attribute = next.metadata().featureOfInterest();
				}
				if (col.equals(SOSConnector.SOS_VALUE_COL_NAME)) {
					attribute = next.value();
				}
				ob.put(col, attribute);
			}
			
			row = new TableRow(columns, ob);
		}
		return row;
	}

}
