package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;
import gov.usgs.cida.nar.util.JNDISingleton;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.StreamResponse;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.plan.Plan;
import gov.usgs.cida.nude.plan.PlanStep;
import gov.usgs.webservices.framework.basic.MimeType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import javax.xml.stream.XMLStreamException;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;


public class SosAggregationService {
	
	private static final Logger log = Logger.getLogger(SosAggregationService.class);
	
	private DownloadType type;
	private String sosUrl;
	private String observedPropertyPrefix;
	
	public SosAggregationService(DownloadType type, String sosUrl, String observedPropertyPrefix) {
		this.type = type;
		this.sosUrl = sosUrl;
		this.observedPropertyPrefix = observedPropertyPrefix;
	}
	
	public void streamData(OutputStream output,
			final MimeType mimeType,
			final List<String> dataType,
			final List<String> qwDataType,
			final List<String> streamFlowType,
			final List<String> constituent,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state,
			final List<String> startDateTime,
			final List<String> endDateTime) throws IOException {
		
		final List<SOSConnector> sosConnectors = getSosConnectors(
				sosUrl,
				dataType,
				qwDataType,
				streamFlowType,
				constituent,
				siteType,
				stationId,
				state,
				startDateTime,
				endDateTime);
		
		// just one for now
		final SOSConnector sosConnector = sosConnectors.get(0);
		
		List<PlanStep> steps = new LinkedList<>();
		PlanStep connectorStep;
		connectorStep = new PlanStep() {
			
			@Override
			public ResultSet runStep(ResultSet rs) {
				while (!sosConnector.isReady()) {
					try {
						Thread.sleep(1000);
					}
					catch (InterruptedException ex) {
						log.debug(ex);
					}
				}
				return sosConnector.getResultSet();
			}

			@Override
			public ColumnGrouping getExpectedColumns() {
				return sosConnector.getExpectedColumns();
			}
		};
		
		steps.add(connectorStep);

		Plan plan = new Plan(steps);
		
		ResultSet runStep = Plan.runPlan(plan);
		TableResponse tr = new TableResponse(runStep);
		StreamResponse sr = null;
		try {
			sr = Dispatcher.buildFormattedResponse(mimeType, tr);
		} catch (IOException| SQLException | XMLStreamException ex) {
			log.error("Unable to build formatted response", ex);
		}
		if (sr != null && output != null) {
			if (mimeType == MimeType.CSV || mimeType == MimeType.TAB) {
				output.write(DescriptionLoaderSingleton.getDescription(type.getTitle()).getBytes());
			}
			StreamResponse.dispatch(sr, new PrintWriter(output));
			output.flush();
			sosConnector.close();
		}
	}
	
	public List<SOSConnector> getSosConnectors(final String sosUrl,
			final List<String> dataType,
			final List<String> qwDataType,
			final List<String> streamFlowType,
			final List<String> constituent,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state,
			final List<String> startDateTime,
			final List<String> endDateTime) {
		List<SOSConnector> sosConnectors = new ArrayList<>();
		//Use the constituent list as the observed properties unless the enum has a list
		List<String> observedProperties = this.type.getObservedProperties();
		if(observedProperties == null) {
			observedProperties = new ArrayList<>();
			for(String prop : constituent) {
				observedProperties.add(this.observedPropertyPrefix + prop);
			}
		}
		
		for(String procedure : this.type.getProcedures()) {
			final SOSConnector sosConnector = new SOSConnector(sosUrl, 
					null, 
					null, 
					observedProperties, 
					Arrays.asList(procedure),
					stationId);
			sosConnectors.add(sosConnector);
		}
		return sosConnectors;
	}
	
}
