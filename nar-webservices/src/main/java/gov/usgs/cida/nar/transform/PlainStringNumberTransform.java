package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class PlainStringNumberTransform implements ColumnTransform {
	
	private static final Logger log = LoggerFactory.getLogger(PlainStringNumberTransform.class);
	
	private final Column inColumn;
	
	public PlainStringNumberTransform(Column inColumn) {
		this.inColumn = inColumn;
	}

	@Override
	public String transform(TableRow row) {
		String value = row.getValue(inColumn);
		try {
			BigDecimal bigDecimal = new BigDecimal(value);
			value = bigDecimal.toPlainString();
		} catch (NullPointerException | NumberFormatException ex) {
			log.trace("Unable to get number value from column");
		}
		return value;
	}

}
