package gov.usgs.cida.nar.util;

import gov.usgs.cida.nar.service.DataType;
import gov.usgs.cida.nar.service.DownloadType;

import java.util.List;

public class ServiceParameterUtils {
	public static String CSV_DELIMITER = ",";
	public static String TSV_DELIMITER = "\t";
	
	public static String getDelimiterFromFormat(final List<String> format) {
		String delimiter = CSV_DELIMITER;
		if(format != null && format.get(0).equalsIgnoreCase("tsv")) {
			delimiter = TSV_DELIMITER;
		}
		return delimiter;
	}
	
	public static boolean isSiteInformationRequested(final List<String> dataTypes) {
		if(dataTypes != null) {
			if(dataTypes.contains(DataType.siteInformation.name())) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean isDiscreteQwRequested(final List<String> inDataTypes, final List<String> inQwDataTypes) {
		return isQwParamRequested(inDataTypes, inQwDataTypes, DownloadType.discreteQw);
	}

	public static boolean isAnnualLoadsRequested(final List<String> inDataTypes, final List<String> inQwDataTypes) {
		return isQwParamRequested(inDataTypes, inQwDataTypes, DownloadType.annualLoad);
	}
	
	public static boolean isMonthlyLoadsRequested(final List<String> inDataTypes, final List<String> inQwDataTypes) {
		return isQwParamRequested(inDataTypes, inQwDataTypes, DownloadType.monthlyLoad);
	}
	
	private static boolean isQwParamRequested(final List<String> dataTypes, final List<String> qwDataTypes, DownloadType qwParamType) {
		if(dataTypes != null) {
			if(dataTypes.contains(DataType.waterQuality.name())) {
				if(qwDataTypes == null || qwDataTypes.size() == 0 || qwDataTypes.contains(qwParamType.name())) {
					return true;
				}
			}
		}
		return false;
	}

	
	public static boolean isAnnualFlowRequested(final List<String> inDataTypes, final List<String> inStreamFlowTypes) {
		return isFlowParamRequested(inDataTypes, inStreamFlowTypes, DownloadType.annualFlow);
	}

	public static boolean isMonthlyFlowRequested(final List<String> inDataTypes, final List<String> inStreamFlowTypes) {
		return isFlowParamRequested(inDataTypes, inStreamFlowTypes, DownloadType.monthlyFlow);
	}
	
	public static boolean isDailyFlowRequested(final List<String> inDataTypes, final List<String> inStreamFlowTypes) {
		return isFlowParamRequested(inDataTypes, inStreamFlowTypes, DownloadType.dailyFlow);
	}
	
	private static boolean isFlowParamRequested(final List<String> dataTypes, final List<String> streamFlowTypes, DownloadType flowParamType) {
		if(dataTypes != null) {
			if(dataTypes.contains(DataType.streamFlow.name())) {
				if(streamFlowTypes == null || streamFlowTypes.size() == 0 || streamFlowTypes.contains(flowParamType.name())) {
					return true;
				}
			}
		}
		return false;
	}
}
