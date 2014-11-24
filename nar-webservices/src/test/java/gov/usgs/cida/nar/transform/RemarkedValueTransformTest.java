package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.resultset.ResultSetUtils;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.filter.FilterStageBuilder;
import gov.usgs.cida.nude.filter.NudeFilterBuilder;
import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author thongsav
 */
public class RemarkedValueTransformTest {
	private static final Logger log = LoggerFactory.getLogger(RemarkedValueTransformTest.class);
	
	public RemarkedValueTransformTest() {
	}
	
	@BeforeClass
	public static void setUpClass() {
		valueColumn = new SimpleColumn("procedure");
		
		inputColumns = new ColumnGrouping(Arrays.asList(new Column[] {
			new SimpleColumn("SITE_QW_ID"),
			new SimpleColumn("CONSTIT"),
			new SimpleColumn("MODTYPE"),
			new SimpleColumn("DATE"),
			new SimpleColumn("procedure")
		}));
		
		inputSampleDataset = ResultSetUtils.createTableRows(inputColumns, new String[][] {
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1979-12-31T18:00:00.000-06:00","<1.143605947"},
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1980-12-31T18:00:00.000-06:00",">4585894.2"},
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1981-12-31T18:00:00.000-06:00","<7674767.59"},
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1982-12-31T18:00:00.000-06:00","8882042.96"},
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1983-12-31T18:00:00.000-06:00","1.325656857"},
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1984-12-31T18:00:00.000-06:00","5864574.53"},
			new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1985-12-31T18:00:00.000-06:00","<8600366.26"}
		});
		
		expectedSampleValuesDataset = ResultSetUtils.createTableRows(inputColumns, new String[][] {
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1979-12-31T18:00:00.000-06:00","1.143605947"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1980-12-31T18:00:00.000-06:00","4585894.2"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1981-12-31T18:00:00.000-06:00","7674767.59"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1982-12-31T18:00:00.000-06:00","8882042.96"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1983-12-31T18:00:00.000-06:00","1.325656857"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1984-12-31T18:00:00.000-06:00","5864574.53"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1985-12-31T18:00:00.000-06:00","8600366.26"}
		});
		
		expectedSampleRemarksDataset = ResultSetUtils.createTableRows(inputColumns, new String[][] {
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1979-12-31T18:00:00.000-06:00","<"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1980-12-31T18:00:00.000-06:00",">"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1981-12-31T18:00:00.000-06:00","<"},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1982-12-31T18:00:00.000-06:00",""},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1983-12-31T18:00:00.000-06:00",""},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1984-12-31T18:00:00.000-06:00",""},
				new String[] {"01646580","http://cida.usgs.gov/def/NAR/property/Q","annual_flow","1985-12-31T18:00:00.000-06:00","<"}
		});
	}
	
	@AfterClass
	public static void tearDownClass() {
	}
	
	@Before
	public void setUp() {
	}
	
	@After
	public void tearDown() {
	}

	/**
	 * Test of transform method, of class RemarkedValueTransform.
	 */
	@Test
	public void testTransform() {
		log.debug("transform");
		TableRow row = new TableRow(valueColumn, "<0.1");
		TableRow row2 = new TableRow(valueColumn, "9.1");
		
		RemarkedValueTransform valueInstance = new RemarkedValueTransform(valueColumn, false);
		RemarkedValueTransform remarkInstance = new RemarkedValueTransform(valueColumn, true);
		
		assertEquals("0.1", valueInstance.transform(row));
		assertEquals("<", remarkInstance.transform(row));

		assertEquals("9.1", valueInstance.transform(row2));
		assertEquals("", remarkInstance.transform(row2));
	}

	/**
	 * Test of transform method, of class RemarkedValueTransform.
	 */
	@Test
	public void testSampleValuesDataset() throws SQLException {
		log.debug("sampleDataset");
		ResultSet expected = new IteratorWrappingResultSet(expectedSampleValuesDataset.iterator());
		ResultSet in = new IteratorWrappingResultSet(inputSampleDataset.iterator());
		ResultSet actual = new NudeFilterBuilder(inputColumns)
				.addFilterStage(new FilterStageBuilder(inputColumns)
						.addTransform(valueColumn, new RemarkedValueTransform(valueColumn, false))
						.buildFilterStage())
				.buildFilter().filter(in);
		assertTrue(ResultSetUtils.checkEqualRows(expected, actual));
	}
	
	/**
	 * Test of transform method, of class RemarkedValueTransform.
	 */
	@Test
	public void testSampleRemarksDataset() throws SQLException {
		log.debug("sampleDataset");
		ResultSet expected = new IteratorWrappingResultSet(expectedSampleRemarksDataset.iterator());
		ResultSet in = new IteratorWrappingResultSet(inputSampleDataset.iterator());
		ResultSet actual = new NudeFilterBuilder(inputColumns)
				.addFilterStage(new FilterStageBuilder(inputColumns)
						.addTransform(valueColumn, new RemarkedValueTransform(valueColumn, true))
						.buildFilterStage())
				.buildFilter().filter(in);
		assertTrue(ResultSetUtils.checkEqualRows(expected, actual));
	}
	
	public static Column valueColumn = null;
	
	public static ColumnGrouping inputColumns = null;
	public static Iterable<TableRow> inputSampleDataset = null;
	public static Iterable<TableRow> expectedSampleValuesDataset = null;
	public static Iterable<TableRow> expectedSampleRemarksDataset = null;
}
