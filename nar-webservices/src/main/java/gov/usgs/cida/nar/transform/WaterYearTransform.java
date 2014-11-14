package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author dmsibley
 */
public class WaterYearTransform implements ColumnTransform {
	private static final Logger log = LoggerFactory.getLogger(WaterYearTransform.class);

	protected final Column inColumn;

	public WaterYearTransform(Column inColumn) {
		this.inColumn = inColumn;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		
		if(null != row) {
			String in = row.getValue(inColumn);
			try {
				DateTime inDate = ISODateTimeFormat.dateTimeParser().parseDateTime(in).withZone(DateTimeZone.UTC);
				int year = inDate.getYear();
				
				DateTime nextWaterYearStart = new DateTime(year, 10, 1, 0, 0, DateTimeZone.UTC);
				DateTime nextWaterYearEnd = new DateTime(year + 1, 10, 1, 0, 0, DateTimeZone.UTC);
				Interval nextWaterYear = new Interval(nextWaterYearStart, nextWaterYearEnd);
				if (nextWaterYear.contains(inDate)) {
					year++;
				}
				
				result = "" + year;
			} catch (Exception e) {
				log.trace("Could not parse incoming value", e);
			}
			
			if (null == result) {
				result = in;
			}
		}
		
		return result;
	}

}
