package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author thongsav
 */
public class RemarkedValueTransform implements ColumnTransform {
	private static final Logger log = LoggerFactory.getLogger(RemarkedValueTransform.class);

	private static final String[] REMARKS = new String[]{ "<", ">" };
	
	protected final Column inColumn;
	
	/*
	 * this transformer returns the value portion and strips out the remark unless this boolean is set to true
	 */
	protected boolean getRemark;

	public RemarkedValueTransform(Column inColumn, boolean getRemark) {
		this.inColumn = inColumn;
		this.getRemark = getRemark;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		
		if(null != row) {
			String in = row.getValue(inColumn);
			try {
				if(getRemark) {
					result = ""; //assume no remark contained
					for(String remark : REMARKS) {
						if(in.contains(remark)) {
							result = remark;
						}
					}
				} else {
					result = in;
					for(String remark : REMARKS) {
						result = result.replace(remark, "");
					}
				}
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
