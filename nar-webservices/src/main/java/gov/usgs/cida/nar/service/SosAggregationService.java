package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.StreamResponse;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.plan.Plan;
import gov.usgs.cida.nude.plan.PlanStep;
import gov.usgs.cida.nude.resultset.inmemory.MuxResultSet;
import gov.usgs.cida.sos.OrderedFilter;
import gov.usgs.webservices.framework.basic.MimeType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.IOUtils;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;


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
			final List<String> constituent,
			final List<String> stationId,
			final String startDateTime,
			final String endDateTime,
			final String header) throws IOException {
		//TODO do something with the header
		
		final List<SOSConnector> sosConnectors = getSosConnectors(
				sosUrl,
				constituent,
				stationId,
				startDateTime,
				endDateTime);
		
		List<PlanStep> steps = new LinkedList<>();
		PlanStep connectorStep;
		connectorStep = new PlanStep() {
			
			@Override
			public ResultSet runStep(ResultSet rs) {
				boolean areAllReady = false;
				List<ResultSet> rsets = new ArrayList<>();
				
				long start = System.currentTimeMillis();
				while (!areAllReady) {
					boolean readyCheck = true;
					int numberReady = 0;
					for (IConnector conn : sosConnectors) {
						boolean connReady = conn.isReady();
						readyCheck = (readyCheck && connReady);
					}
					// TODO make sure isReady() will eventually be true
					log.trace(String.format("Streams complete: {} of {}", ++numberReady, sosConnectors.size()));
					try {
						Thread.sleep(250);
					}
					catch (InterruptedException ex) {
						log.debug(ex);
					}
					areAllReady = readyCheck;
				}
				
				for (IConnector conn : sosConnectors) {
					ResultSet resultSet = conn.getResultSet();
					rsets.add(resultSet);
				}
				
				log.debug("***** Time elapsed: " + (System.currentTimeMillis() - start));
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

		//do necessary transformations by download type
		switch(this.type) {
			case annualLoad:
				addAnnualLoadSteps(steps);
				break;
			case mayLoad:
				addMayLoadSteps(steps);
				break;
			case annualFlow:
				addAnnualFlowSteps(steps);
				break;
			case dailyFlow:
				addDailyFlowSteps(steps);
				break;
			case discreteQw:
				addDiscreteQwSteps(steps);
				break;
			default: //nothing
		}
		
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
			final List<String> constituent,
			final List<String> stationId,
			final String startDateTime,
			final String endDateTime) {
		List<SOSConnector> sosConnectors = new ArrayList<>();
		
		List<String> actualProperties = new ArrayList<>();
		for(String prop : constituent) {
			actualProperties.add(this.observedPropertyPrefix + prop);
		}
		
		DateTime start = null;
		try {
			start = DateTime.parse(startDateTime, DateTimeFormat.forPattern("MM/dd/yyyy"));
		} catch(Exception e) {
			log.debug(e);
		}
		DateTime end = null;
		try {
			end = DateTime.parse(endDateTime, DateTimeFormat.forPattern("MM/dd/yyyy"));
		} catch(Exception e) {
			log.debug(e);
		}
		
		Map<String, List<String>> columnMap = new HashMap<>();
		
		for(String procedure : this.type.getProcedures()) {
			String columnName = DownloadType.getColumnNameFromProcedure(procedure);
			if (columnMap.containsKey(columnName)) {
				List<String> procList = columnMap.get(columnName);
				procList.add(procedure);
			} else {
				List<String> procList = new LinkedList<>();
				procList.add(procedure);
				columnMap.put(columnName, procList);
			}
		}
		
		for (String columnName : columnMap.keySet()) {
			List<String> procList = columnMap.get(columnName);
			SortedSet<OrderedFilter> filters = new TreeSet<>();
			SOSClient sosClient = new SOSClient(sosUrl, start, end, actualProperties, procList, stationId);
			for (String procedure : procList) {
				for (String prop : actualProperties) {
					for (String featureOfInterest : stationId) {
						filters.add(new OrderedFilter(procedure, prop, featureOfInterest));
					}
				}
			}
			final SOSConnector sosConnector = new SOSConnector(sosClient, filters, columnName);
			sosConnectors.add(sosConnector);
		}
		return sosConnectors;
	}
	
	private void addAnnualLoadSteps(List<PlanStep> steps) {
		//TODO
	}
	
	private void addMayLoadSteps(List<PlanStep> steps) {
		//TODO
	}

	private void addAnnualFlowSteps(List<PlanStep> steps) {
		//TODO
	}
	
	private void addDailyFlowSteps(List<PlanStep> steps) {
		//TODO
	}

	private void addDiscreteQwSteps(List<PlanStep> steps) {
		//TODO
	}
	
}
