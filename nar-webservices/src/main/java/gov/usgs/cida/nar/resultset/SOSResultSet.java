package gov.usgs.cida.nar.resultset;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.Observation;
import gov.usgs.cida.sos.ObservationCollection;
import gov.usgs.cida.sos.OrderedFilter;
import gov.usgs.cida.sos.WaterML2Parser;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSResultSet extends OGCResultSet {
	
	private static final Logger log = LoggerFactory.getLogger(SOSResultSet.class);
	
	private SortedSet<OrderedFilter> filters;
	private SOSClient client;
	private ObservationCollection currentCollection;
	private InputStream sourceStream;

	public SOSResultSet(SortedSet<OrderedFilter> filters, SOSClient client, ColumnGrouping colGroups) {
		this.filters = filters;
		this.client = client;
		this.columns = colGroups;
	}

	@Override
	public void close() throws SQLException {
		IOUtils.closeQuietly(currentCollection);
		IOUtils.closeQuietly(sourceStream);
		super.close();
	}
	
	private ObservationCollection nextCollection() throws XMLStreamException {
		ObservationCollection collection = null;
		sourceStream = this.client.readFile();
		WaterML2Parser parser = new WaterML2Parser(sourceStream);
		if (filters.size() > 0) {
			OrderedFilter first = filters.first();
			filters.remove(first);
			collection = parser.getFilteredObservations(first);
		}
		return collection;
	}

	@Override
	protected TableRow makeNextRow() {
		TableRow row = null;
		if (currentCollection != null && currentCollection.hasNext()) {
			Observation next = currentCollection.next();
			Map<Column, String> ob = new HashMap<>();
			for (Column col : columns) {
				String attribute = null;
				if (col.equals(SOSConnector.SOS_DATE_COL)) {
					attribute = next.time().toString();
				}
				else if (col.equals(SOSConnector.SOS_MOD_TYPE_COL)) {
					attribute = DownloadType.getModTypeFromProcedure(next.metadata().procedure());
				}
				else if (col.equals(SOSConnector.SOS_CONSTITUENT_COL)) {
					attribute = next.metadata().observedProperty();
				}
				else if (col.equals(SOSConnector.SOS_SITE_COL)) {
					attribute = next.metadata().featureOfInterest();
				}
				else {
					attribute = next.value();
				}
				ob.put(col, attribute);
			}
			
			row = new TableRow(columns, ob);
		} else {
			IOUtils.closeQuietly(currentCollection);
			IOUtils.closeQuietly(sourceStream);
			try {
				currentCollection = nextCollection();
			}
			catch (XMLStreamException ex) {
				log.error("Error reading xml stream", ex);
			}
			if (currentCollection != null) {
				row = makeNextRow();
			}
		}
		return row;
	}

}
