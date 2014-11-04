package gov.usgs.cida.nar.connector;

import gov.usgs.cida.nar.resultset.SOSResultSet;
import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.connector.IConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.filter.FilterStage;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.FilteredResultSet;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.sos.WaterML2Parser;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class SOSConnector implements IConnector, Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(SOSConnector.class);

	public static final Column COMPOSITE_KEY_COL = new SimpleColumn("COMPOSITE", true);
	public static final Column SOS_SITE_COL = new SimpleColumn("SITE_QW_ID");
	public static final Column SOS_CONSTITUENT_COL = new SimpleColumn("CONSTIT");
	public static final Column SOS_MOD_TYPE_COL = new SimpleColumn("MODTYPE");
	public static final Column SOS_DATE_COL = new SimpleColumn("DATE");
	
	private static final String NUMERIC_SUFFIX = "_NUMERIC";
	
	private SOSClient client;
	private ColumnGrouping cg;
	private String sosEndpoint;
	private DateTime startTime;
	private DateTime endTime;
	private String observedProperty;
	private String procedure;
	private List<String> featuresOfInterest;

	private String modType;
	private String valueColumn;
	
	private boolean isReady;


	public SOSConnector(String sosEndpoint, DateTime startTime, DateTime endTime, String observedProperty,
			String procedure, List<String> featuresOfInterest) {
		this.sosEndpoint = sosEndpoint;
		this.startTime = startTime;
		this.endTime = endTime;
		this.observedProperty = observedProperty;
		this.procedure = procedure;
		this.featuresOfInterest = featuresOfInterest;
		this.isReady = false;
		
		this.modType = DownloadType.getModTypeFromProcedure(procedure);
		this.valueColumn = DownloadType.getColumnNameFromProcedure(procedure);
		
		this.client = new SOSClient(sosEndpoint, startTime, endTime, Arrays.asList(observedProperty), Arrays.asList(procedure), featuresOfInterest);
		this.cg = makeColumnGrouping(valueColumn);
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
		
		FilterStage makeNumericColumnsStage = new FilterStageBuilder(getExpectedColumns())
				.addTransform(new SimpleColumn(SOS_SITE_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_SITE_COL);
						int orderedVal = value.length() * Integer.parseInt(value);
						String stringVal = String.valueOf(orderedVal);
						return stringVal;
					}
				})
				.addTransform(new SimpleColumn(SOS_CONSTITUENT_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_CONSTITUENT_COL);
						String orderedVal = "" + Math.abs(value.hashCode());
						return StringUtils.leftPad(orderedVal.substring(0, 4), 4, "0");
					}
				})
				.addTransform(new SimpleColumn(SOS_MOD_TYPE_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_MOD_TYPE_COL);
						String orderedVal = "" + Math.abs(value.hashCode());
						return StringUtils.leftPad(orderedVal.substring(0, 4), 4, "0");
					}
				})
				.addTransform(new SimpleColumn(SOS_DATE_COL.getName() + NUMERIC_SUFFIX, false), new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String value = row.getValue(SOS_DATE_COL);
						DateTime parsed = DateTime.parse(value);
						int padTo = String.valueOf(DateTime.now().getMillis()/1000).length();
						return StringUtils.leftPad(String.valueOf(parsed.getMillis()/1000), padTo, "0");
					}
				})
				.buildFilterStage();
		ResultSet numericColumnResultSet = new FilteredResultSet(resultSet, makeNumericColumnsStage);
		
		FilterStage makePrimaryKeyStage = new FilterStageBuilder(makeNumericColumnsStage.getOutputColumns())
				.addTransform(COMPOSITE_KEY_COL, new ColumnTransform() {
					@Override
					public String transform(TableRow row) {
						String siteVal = row.getValue(new SimpleColumn(SOS_SITE_COL.getName() + NUMERIC_SUFFIX));
						String constituentVal = row.getValue(new SimpleColumn(SOS_CONSTITUENT_COL.getName() + NUMERIC_SUFFIX));
						String modtypeVal = row.getValue(new SimpleColumn(SOS_MOD_TYPE_COL.getName() + NUMERIC_SUFFIX));
						String dateVal = row.getValue(new SimpleColumn(SOS_DATE_COL.getName() + NUMERIC_SUFFIX));
						String compositeVal = siteVal + constituentVal + modtypeVal + dateVal;
						return compositeVal;
					}
				})
				.buildFilterStage();
		
		ResultSet numericPrimaryKeyResultSet = new FilteredResultSet(numericColumnResultSet, makePrimaryKeyStage);
		
		return numericPrimaryKeyResultSet;
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
	
	private static ColumnGrouping makeColumnGrouping(String valueColumn) {
		Column primaryKey = COMPOSITE_KEY_COL;
		List<Column> allColumns = new LinkedList<>();
		allColumns.add(primaryKey);
		allColumns.add(SOS_SITE_COL);
		allColumns.add(SOS_CONSTITUENT_COL);
		allColumns.add(SOS_MOD_TYPE_COL);
		allColumns.add(SOS_DATE_COL);
		allColumns.add(new SimpleColumn(valueColumn));
		return new ColumnGrouping(primaryKey, allColumns);
	}
	
	@Override
	public void close() {
		client.close();
	}

}
