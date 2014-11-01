package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.resultset.SOSResultSet;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.sos.WaterML2Parser;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSConnector implements IConnector {
	
	private static final Logger log = LoggerFactory.getLogger(SOSConnector.class);

	public static final Column SOS_DATE_COL_NAME = new SimpleColumn("DATE");
	public static final Column SOS_PROCEDURE_COL_NAME = new SimpleColumn("PROCEDURE");
	public static final Column SOS_CONSTITUENT_COL_NAME = new SimpleColumn("CONSTIT");
	public static final Column SOS_SITE_COL_NAME = new SimpleColumn("SITE_QW_ID");
	public static final Column SOS_VALUE_COL_NAME = new SimpleColumn("VALUE");
	
	private SOSClient client;
	private ColumnGrouping cg;
	private String sosEndpoint;
	private DateTime startTime;
	private DateTime endTime;
	private List<String> observedProperties;
	private List<String> procedures;
	private List<String> featuresOfInterest;

	private boolean isReady;


	public SOSConnector(String sosEndpoint, DateTime startTime, DateTime endTime, List<String> observedProperties,
			List<String> procedures, List<String> featuresOfInterest) {
		this.sosEndpoint = sosEndpoint;
		this.startTime = startTime;
		this.endTime = endTime;
		this.observedProperties = observedProperties;
		this.procedures = procedures;
		this.featuresOfInterest = featuresOfInterest;
		this.isReady = false;
		
		this.client = new SOSClient(sosEndpoint, startTime, endTime, observedProperties, procedures, featuresOfInterest);
		this.cg = makeColumnGrouping();
	}

	@Override
	public void addInput(ResultSet in) {
		return;
	}

	@Override
	public String getStatement() {
		return null;
	}

	@Override
	public ResultSet getResultSet() {
		ResultSet resultSet = null;
		
		if (!isReady()) {
			throw new RuntimeException("Not ready yet");
		}
		WaterML2Parser parser = new WaterML2Parser(this.client.readFile());
		try {
			resultSet = new SOSResultSet(parser.getObservations(), this.cg);
		} catch (FileNotFoundException | XMLStreamException ex) {
			log.error("Cannot read stream", ex);
			resultSet = new StringTableResultSet(getExpectedColumns());
		}
		
		return resultSet;
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
		if (client.getState() == Thread.State.NEW) {
			client.start();
		} else if (client.getState() == Thread.State.TERMINATED) {
			this.isReady = true;
		}
		
		return this.isReady;
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		return this.cg;
	}
	
	private static ColumnGrouping makeColumnGrouping() {
		Column primaryKey = SOS_DATE_COL_NAME;
		List<Column> allColumns = new LinkedList<>();
		allColumns.add(primaryKey);
		allColumns.add(SOS_PROCEDURE_COL_NAME);
		allColumns.add(SOS_CONSTITUENT_COL_NAME);
		allColumns.add(SOS_SITE_COL_NAME);
		allColumns.add(SOS_VALUE_COL_NAME);
		return new ColumnGrouping(primaryKey, allColumns);
	}
	
	public void close() {
		client.close();
	}

}
