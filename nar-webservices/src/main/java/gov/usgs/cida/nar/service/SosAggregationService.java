package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.StreamResponse;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.plan.Plan;
import gov.usgs.cida.nude.plan.PlanStep;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
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
import org.apache.commons.io.IOUtils;

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
			final String startDateTime,
			final String endDateTime,
			final String header) throws IOException {
		//TODO do something with the header
		
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
		
		List<PlanStep> steps = new LinkedList<>();
		PlanStep connectorStep;
		connectorStep = new PlanStep() {
			
			@Override
			public ResultSet runStep(ResultSet rs) {
				//boolean areAllReady = false;
				List<ResultSet> rsets = new ArrayList<>();
				//while (!areAllReady) {
//					boolean readyCheck = true;
//					int numberReady = 0;
				for (IConnector conn : sosConnectors) {

					while (!conn.isReady()) {
//						if (connReady) {
//							numberReady++;
//						}
//						readyCheck = (readyCheck && connReady);
					// TODO make sure isReady() will eventually be true
						//}
						try {
							Thread.sleep(250);
						}
						catch (InterruptedException ex) {
							log.debug(ex);
						}
//					areAllReady = readyCheck;
					}
				}
				
				for (IConnector conn : sosConnectors) {
					ResultSet resultSet = conn.getResultSet();
					rsets.add(resultSet);
				}
				
				return new MuxResultSet(rsets);
			}

			@Override
			public ColumnGrouping getExpectedColumns() {
				List<ColumnGrouping> cgs = new ArrayList<>();
				for (IConnector conn : sosConnectors) {
					cgs.add(conn.getExpectedColumns());
				}
				return ColumnGrouping.join(cgs);
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
			StreamResponse.dispatch(sr, new PrintWriter(output));
			output.flush();
			for (SOSConnector conn : sosConnectors) {
				IOUtils.closeQuietly(conn);
			}
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
			final String startDateTime,
			final String endDateTime) {
		List<SOSConnector> sosConnectors = new ArrayList<>();
		
		List<String> actualProperties = new ArrayList<>();
		for(String prop : constituent) {
			actualProperties.add(this.observedPropertyPrefix + prop);
		}
		
		DateTime start = null;
		try {
			start = new DateTime(startDateTime);
		} catch(Exception e) {
			log.debug(e);
		}
		DateTime end = null;
		try {
			end = new DateTime(endDateTime);
		} catch(Exception e) {
			log.debug(e);
		}
		
		
		for(String procedure : this.type.getProcedures()) {
			final SOSConnector sosConnector = new SOSConnector(sosUrl, 
					start, 
					end, 
					actualProperties, 
					procedure,
					stationId);
			sosConnectors.add(sosConnector);
		}
		return sosConnectors;
	}
	
}
