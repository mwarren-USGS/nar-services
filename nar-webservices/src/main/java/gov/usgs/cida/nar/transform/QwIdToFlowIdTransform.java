package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nar.service.SiteInformationService;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import org.geotools.data.simple.SimpleFeatureCollection;

/**
 * 
 * @author thongsav
 */
public class QwIdToFlowIdTransform implements ColumnTransform {
	protected final Column inColumn;
	protected final SimpleFeatureCollection siteFeatures;
	
	public QwIdToFlowIdTransform(Column inColumn, SimpleFeatureCollection siteFeatures) {
		this.inColumn = inColumn;
		this.siteFeatures = siteFeatures;
	}
	
	@Override
	public String transform(TableRow row) {
		String qwId = row.getValue(inColumn);
		return SiteInformationService.getFlowIdFromQwId(siteFeatures, qwId);
	}

}
