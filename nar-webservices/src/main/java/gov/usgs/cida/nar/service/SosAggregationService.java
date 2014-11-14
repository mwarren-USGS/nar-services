package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.connector.SOSClient;
import gov.usgs.cida.nar.connector.SOSConnector;
import gov.usgs.cida.nar.transform.PrefixStripTransform;
import gov.usgs.cida.nar.transform.ToDayDateTransform;
import gov.usgs.cida.nar.transform.WaterYearTransform;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilterStep;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.filter.transform.ColumnAlias;
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
	
	private static final String SITE_QW_ID_IN_COL = "SITE_QW_ID";
	private static final String DATE_IN_COL = "DATE";
	
	private static final String WY_OUT_COL = "WY";
	private static final String FLOW_OUT_COL = "FLOW";
	
	private static final String SITE_FLOW_ID_IN_COL = "SITE_FLOW_ID"; 
	private static final String SITE_CONSTIT_IN_COL = "CONSTIT";
	private static final String SITE_CONCENTRATION_IN_COL = "CONCENTRATION";
	private static final String SITE_REMARK_IN_COL = "REMARK";
	
	private static final String PROPERTY_PREFIX = "http://cida.usgs.gov/def/NAR/property/";
	
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
				steps.addAll(getAnnualLoadSteps(steps));
				break;
			case mayLoad:
				steps.addAll(getMayLoadSteps(steps));
				break;
			case annualFlow:
				steps.addAll(getAnnualFlowSteps(steps));
				break;
			case dailyFlow:
				steps.addAll(getDailyFlowSteps(steps));
				break;
			case discreteQw:
				steps.addAll(getDiscreteQwSteps(steps));
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
	
	private List<PlanStep> getAnnualLoadSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		//TODO
		return steps;
	}
	
	private List<PlanStep> getMayLoadSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		//TODO
		return steps;
	}

	private List<PlanStep> getAnnualFlowSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		
		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(5)))
							.addTransform(new SimpleColumn(FLOW_OUT_COL), new ColumnAlias(originals.get(6)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);

		//drop constit and modtype columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, FLOW_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);

		//convert date to WY
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));

		return steps;
	}
	
	private List<PlanStep> getDailyFlowSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		
		//rename columns to specified headers
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
						.addFilterStage(new FilterStageBuilder(originals)
							.addTransform(new SimpleColumn(FLOW_OUT_COL), new ColumnAlias(originals.get(6)))
							.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(5)))
							.buildFilterStage())
				.buildFilter());
		steps.add(renameColsStep);
		
		//drop constit and modtype columns
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, DATE_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, FLOW_OUT_COL)));
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
						.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);
		
		//convert dates to WY and day
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL)), new ToDayDateTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL))))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));

		return steps;
	}

	private List<PlanStep> getDiscreteQwSteps(final List<PlanStep> prevSteps) {
		List<PlanStep> steps = new ArrayList<>();
		//rename columns to specified headers
		//Not sure if any renaming is necessary until missing columns are available 
		ColumnGrouping originals = prevSteps.get(prevSteps.size()-1).getExpectedColumns();
		FilterStep renameColsStep = new FilterStep(new NudeFilterBuilder(originals)
			.addFilterStage(new FilterStageBuilder(originals)
			.addTransform(new SimpleColumn(WY_OUT_COL), new ColumnAlias(originals.get(5)))
			.addTransform(new SimpleColumn(SITE_CONCENTRATION_IN_COL), new ColumnAlias(originals.get(6)))
			.buildFilterStage())
			.buildFilter());
		steps.add(renameColsStep);
		
		//missing cols commented out until available 
		List<Column> finalColList = new ArrayList<>();
		List<Column> allCols = renameColsStep.getExpectedColumns().getColumns();
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_QW_ID_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_CONSTIT_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, DATE_IN_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, WY_OUT_COL)));
		finalColList.add(allCols.get(indexOfCol(allCols, SITE_CONCENTRATION_IN_COL)));
		//TODO NEED REMARK
		
		ColumnGrouping finalCols = new ColumnGrouping(finalColList);
		FilterStep removeUnusedColsStep = new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.buildFilterStage())
				.buildFilter());
		steps.add(removeUnusedColsStep);
		
		//convert dates to WY and day
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL)), new ToDayDateTransform(finalColList.get(indexOfCol(finalColList, DATE_IN_COL))))
				.addTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL)), new WaterYearTransform(finalColList.get(indexOfCol(finalColList, WY_OUT_COL))))
				.buildFilterStage())
		.buildFilter()));
		
		//Strip out the constituent prefix
		steps.add(new FilterStep(new NudeFilterBuilder(finalCols)
				.addFilterStage(new FilterStageBuilder(finalCols)
				.addTransform(finalColList.get(indexOfCol(finalColList, SITE_CONSTIT_IN_COL)), 
						new PrefixStripTransform(finalColList.get(indexOfCol(finalColList, SITE_CONSTIT_IN_COL)), PROPERTY_PREFIX))
				.buildFilterStage())
		.buildFilter()));
		
		return steps;
	}
	
	
	/**
	 * Helper function to get the index of a column with the given name
	 */
	private int indexOfCol(List<Column> cols, String colName) {
		int index = -1;
		for(int i = 0; i < cols.size(); i++) {
			if(cols.get(i).getName().equals(colName)) {
				index = i;
				break;
			}
		}
		return index;
	}
}
