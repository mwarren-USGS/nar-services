package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

/**
 * @author thongsav
 */
public class PrefixStripTransform implements ColumnTransform {
	protected final Column inColumn;
	protected final String prefix;

	public PrefixStripTransform(Column inColumn, String prefix) {
		this.inColumn = inColumn;
		this.prefix = prefix;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		
		if(null != row) {
			String in = row.getValue(inColumn);
			
			if(in != null && in.startsWith(prefix)) {
				result = in.substring(prefix.length());
			} else {
				result = in;
			}
		}
		
		return result;
	}

}
