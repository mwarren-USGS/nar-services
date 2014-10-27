package gov.usgs.cida.nar.util;

import gov.usgs.cida.nar.service.DataType;
import gov.usgs.cida.nar.service.DownloadServiceParameters;
import gov.usgs.cida.nar.service.DownloadType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServiceParameterUtils {
	public static String CSV_DELIMITER = ",";
	public static String TSV_DELIMITER = "\t";
	
	public static String getDelimiterFromFormat(final Map<String, String[]> params) {
		String delimiter = CSV_DELIMITER;
		String[] format = params.get(DownloadServiceParameters.format.name());
		if(format != null && format[0].equalsIgnoreCase("tsv")) {
			delimiter = TSV_DELIMITER;
		}
		return delimiter;
	}
	
	public static boolean isSiteInformationRequested(final Map<String, String[]> params) {
		List<String> dataTypes = getListFromParams(params, DownloadServiceParameters.dataType.name());
		if(dataTypes.contains(DataType.siteInformation.name())) {
			return true;
		}
		return false;
	}
	
	public static boolean isDiscreteQwRequested(final Map<String, String[]> params) {
		return isQwParamRequested(params, DownloadType.discreteQw);
	}

	public static boolean isAnnualLoadsRequested(final Map<String, String[]> params) {
		return isQwParamRequested(params, DownloadType.annualLoad);
	}
	
	public static boolean isMonthlyLoadsRequested(final Map<String, String[]> params) {
		return isQwParamRequested(params, DownloadType.monthlyLoad);
	}
	
	private static boolean isQwParamRequested(final Map<String, String[]> params, DownloadType qwParamType) {
		List<String> dataTypes = getListFromParams(params, DownloadServiceParameters.dataType.name());
		if(dataTypes.contains(DataType.waterQuality.name())) {
			List<String> qwDataTypes = getListFromParams(params, DownloadServiceParameters.qwDataType.name());
			if(qwDataTypes.size() == 0 || qwDataTypes.contains(qwParamType.name())) {
				return true;
			}
		}
		return false;
	}

	
	public static boolean isAnnualFlowRequested(final Map<String, String[]> params) {
		return isFlowParamRequested(params, DownloadType.annualFlow);
	}

	public static boolean isMonthlyFlowRequested(final Map<String, String[]> params) {
		return isFlowParamRequested(params, DownloadType.monthlyFlow);
	}
	
	public static boolean isDailyFlowRequested(final Map<String, String[]> params) {
		return isFlowParamRequested(params, DownloadType.dailyFlow);
	}
	
	private static boolean isFlowParamRequested(final Map<String, String[]> params, DownloadType flowParamType) {
		List<String> dataTypes = getListFromParams(params, DownloadServiceParameters.dataType.name());
		if(dataTypes.contains(DataType.streamFlow.name())) {
			List<String> streamFlowTypes = getListFromParams(params, DownloadServiceParameters.streamFlowType.name());
			if(streamFlowTypes.size() == 0 || streamFlowTypes.contains(flowParamType.name())) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Parameters may come in a combination of comma separated values and query separated values. This function
	 * parses both combinations into a single list.
	 * ie: ?param=one,two,three&param=four becomes a list containing: one, two, three, four
	 * 
	 * @param params parameter map from request
	 * @param paramName the name of the parameter to extract values for
	 * @return
	 */
	public static ArrayList<String> getListFromParams(final Map<String, String[]> params, String paramName) {
		ArrayList<String> paramValues = new ArrayList<>();
		String[] foundParams = params.get(paramName);
		if(foundParams != null) { 
			for(String pFull : foundParams) {
				for(String pSplit : pFull.split(",")) {
					if(!paramValues.contains(pSplit)) {
						paramValues.add(pSplit);
					}
				}
			}
		}
		return paramValues;
	}
}
