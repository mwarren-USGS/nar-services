package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSConnector implements IConnector {
	
	private static final Logger log = LoggerFactory.getLogger(SOSConnector.class);

	private SOSClient client;
	private String sosEndpoint;
	private Date startTime;
	private Date endTime;
	private List<String> observedProperties;
	private List<String> procedures;
	private List<String> featuresOfInterest;

	private boolean isReady;


	public SOSConnector(String sosEndpoint, Date startTime, Date endTime, List<String> observedProperties,
			List<String> procedures, List<String> featuresOfInterest) {
		this.sosEndpoint = sosEndpoint;
		this.startTime = startTime;
		this.endTime = endTime;
		this.observedProperties = observedProperties;
		this.procedures = procedures;
		this.featuresOfInterest = featuresOfInterest;
		this.isReady = false;
		
		this.client = new SOSClient(sosEndpoint, startTime, endTime, observedProperties, procedures, featuresOfInterest);
	}

	@Override
	public void addInput(ResultSet in) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getStatement() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public ResultSet getResultSet() {
		if (!isReady()) {
			throw new RuntimeException("Not ready yet");
		}
		
		
		
		return null;
	}

	@Override
	public IParser getParser() {
		return null;
	}

	@Override
	public boolean isValidInput() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	public void close() {
		client.close();
	}

}
