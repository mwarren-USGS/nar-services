package gov.usgs.cida.nar.parser;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.http.AbstractHttpParser;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO!!! THIS IS NOT A REAL CSV Parser.  Does not use quotes as escapes.
 * @author dmsibley
 */
public class CSVDataParser extends AbstractHttpParser {
	private static final Logger log = LoggerFactory.getLogger(CSVDataParser.class);
	
	protected final String startLinePrefix;
	protected final Column[] columnOrdering;
	protected final DateTimeFormatter dateFormat;
	protected final Column timeColumn;
	
	protected boolean started = false;

	public CSVDataParser(ColumnGrouping cg, String startLinePrefix, Column[] columnOrdering, Column timeColumn, DateTimeFormatter dateFormat) {
		super(cg);
		this.startLinePrefix = startLinePrefix;
		this.columnOrdering = columnOrdering;
		this.dateFormat = dateFormat;
		this.timeColumn = timeColumn;
	}

	@Override
	public boolean next(Reader in) throws SQLException {
		boolean result = false;
		
		BufferedReader inBuf = (BufferedReader) in;
		
		try {
			String ln;
			if (!started) {
				started = true;
				fastForward(inBuf, this.startLinePrefix);
			}

			//ready to get the next row
			ln = inBuf.readLine();

			if (null != ln) {
				String[] vals = ln.split(",", -1);
				if (null != vals && columnOrdering.length <= vals.length) {
					Map<Column, String> row = new HashMap<Column, String>();
					
					for (int i = 0; i < columnOrdering.length; i++) {
						Column col = columnOrdering[i];
						if (null != col) {
							String val = cleanValue(vals[i]);
							
							if (this.timeColumn.equals(col)) {
								val = "" + this.dateFormat.parseDateTime(val).getMillis();
							}
							
							row.put(col, val);
						}
					}

					currRow = new TableRow(cg, row);
					result = true;
				} else {
					if (null != vals) {
						log.debug("Not enough columns from split! Expected:" + columnOrdering.length + ", Actual:" + vals.length);
					} else {
						log.trace("Line cannot be split: " + ln);
					}
				}
			}

		} catch (Exception e) {
			log.debug(this.toString(), e);
			throw new SQLException(e);
		}

		return result;
	}
	
	protected static String fastForward(BufferedReader in, String startsWith) throws IOException {
		String ln = null;
		while (null != (ln = in.readLine()) && !ln.startsWith(startsWith)) {
		}
		return ln;
	}
	
	protected String cleanValue(String in) {
		return StringUtils.trimToNull(in);
	}
}
