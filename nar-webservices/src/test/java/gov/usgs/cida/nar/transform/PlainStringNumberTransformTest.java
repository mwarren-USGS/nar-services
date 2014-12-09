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
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author thongsav
 */
public class PlainStringNumberTransformTest {
	
	private static final Logger log = LoggerFactory.getLogger(PlainStringNumberTransformTest.class);
	
		
	public static Column valueColumn = null;
	
	public static ColumnGrouping inputColumns = null;
	public static Iterable<TableRow> inputSampleDataset = null;
	public static Iterable<TableRow> expectedSampleDataset = null;
	
	public PlainStringNumberTransformTest() {
	}
	
	@BeforeClass
	public static void setUpClass() {
		valueColumn = new SimpleColumn("value");
		
		inputColumns = new ColumnGrouping(Arrays.asList(new Column[] {
			new SimpleColumn("id"),
			new SimpleColumn("value")
		}));
		
		inputSampleDataset = ResultSetUtils.createTableRows(inputColumns, new String[][] {
			new String[] {"1","1.143605948E7"},
			new String[] {"2","4585894.2"},
			new String[] {"3",""},
			new String[] {"4","NA"},
			new String[] {"5","-1.325656857E7"}
		});
		
		expectedSampleDataset = ResultSetUtils.createTableRows(inputColumns, new String[][] {
			new String[] {"1","11436059.48"},
			new String[] {"2","4585894.2"},
			new String[] {"3",""},
			new String[] {"4","NA"},
			new String[] {"5","-13256568.57"}
		});
	}

	/**
	 * Test of transform method, of class PrefixStripTransform.
	 */
	@Test
	public void testTransform() {
		log.debug("transform");
		TableRow row = new TableRow(valueColumn, "1E4");
		PlainStringNumberTransform instance = new PlainStringNumberTransform(valueColumn);
		String expResult = "10000";
		String result = instance.transform(row);
		assertEquals(expResult, result);
	}

	/**
	 * Test of transform method, of class PrefixStripTransform.
	 */
	@Test
	public void testSampleDataset() throws SQLException {
		log.debug("sampleDataset");
		ResultSet expected = new IteratorWrappingResultSet(expectedSampleDataset.iterator());
		ResultSet in = new IteratorWrappingResultSet(inputSampleDataset.iterator());
		ResultSet actual = new NudeFilterBuilder(inputColumns)
				.addFilterStage(new FilterStageBuilder(inputColumns)
						.addTransform(valueColumn, new PlainStringNumberTransform(valueColumn))
						.buildFilterStage())
				.buildFilter().filter(in);
		assertTrue(ResultSetUtils.checkEqualRows(expected, actual));
	}
}
