package gov.usgs.cida.nar.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import gov.usgs.cida.nar.service.DataType;
import gov.usgs.cida.nar.service.DownloadServiceParameters;
import gov.usgs.cida.nar.service.DownloadType;

import java.util.HashMap;

import org.junit.Test;

public class ServiceParameterUtilsTest {

	@Test
	public void testGetDelimiterFromFormat() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertEquals("Defaults to csv", ",", ServiceParameterUtils.getDelimiterFromFormat(params));
		
		params.put(DownloadServiceParameters.format.name(), new String[] { "tsv" });
		assertEquals("Produces tabs when tsv requested", "\t", ServiceParameterUtils.getDelimiterFromFormat(params));
	}
	
	@Test
	public void testGetListFromParams() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertEquals("Empty list returned when param not passed", 0, ServiceParameterUtils.getListFromParams(params, "nonexistantParam").size());
		
		params.put("paramName", new String[] { "one" });
		assertEquals("Single param with single value read", 1, ServiceParameterUtils.getListFromParams(params, "paramName").size());
		assertEquals("Single param with single value read", "one", ServiceParameterUtils.getListFromParams(params, "paramName").get(0));

		params.put("paramName", new String[] { "one,two" });
		assertEquals("Single param with comma separated value read", 2, ServiceParameterUtils.getListFromParams(params, "paramName").size());
		assertEquals("Single param with comma separated value read", "one", ServiceParameterUtils.getListFromParams(params, "paramName").get(0));
		assertEquals("Single param with comma separated value read", "two", ServiceParameterUtils.getListFromParams(params, "paramName").get(1));
		

		params.put("paramName", new String[] { "one,two", "three" });
		assertEquals("Double param with comma separated value read", 3, ServiceParameterUtils.getListFromParams(params, "paramName").size());
		assertEquals("Double param with comma separated value read", "one", ServiceParameterUtils.getListFromParams(params, "paramName").get(0));
		assertEquals("Double param with comma separated value read", "two", ServiceParameterUtils.getListFromParams(params, "paramName").get(1));
		assertEquals("Double param with comma separated value read", "three", ServiceParameterUtils.getListFromParams(params, "paramName").get(2));
	}
	
	@Test
	public void testIsSiteInformationRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Site information not requested if param not provided", ServiceParameterUtils.isSiteInformationRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.siteInformation.name() });
		assertTrue("Site information requeted when parameter " + DownloadServiceParameters.dataType.name() +
				" contains value " + DataType.siteInformation.name(), ServiceParameterUtils.isSiteInformationRequested(params));
		

		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.siteInformation.name(), "mixedinOtherValues" });
		assertTrue("Site information requeted when parameter " + DownloadServiceParameters.dataType.name() +
				" contains value " + DataType.siteInformation.name(), ServiceParameterUtils.isSiteInformationRequested(params));
	}
	
	@Test
	public void testIsDiscreteQwRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Discrete QW not requested if params not provided", ServiceParameterUtils.isDiscreteQwRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.waterQuality.name() });
		assertTrue("Discrete QW requested if water quality data type requested without any QW data type filters", ServiceParameterUtils.isDiscreteQwRequested(params));

		params.put(DownloadServiceParameters.qwDataType.name(), new String[] { DownloadType.annualLoad.name() });
		assertFalse("Discrete QW NOT requested if water quality data type requested with a QW data type filters that do not include discrete QW", ServiceParameterUtils.isDiscreteQwRequested(params));

		params.put(DownloadServiceParameters.qwDataType.name(), new String[] { DownloadType.annualLoad.name(), DownloadType.discreteQw.name() });
		assertTrue("Discrete QW requested if water quality data type requested with a QW data type filters that include discrete QW", ServiceParameterUtils.isDiscreteQwRequested(params));
	}
	
	@Test
	public void testIsAnnualLoadsRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Annual not requested if params not provided", ServiceParameterUtils.isAnnualLoadsRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.waterQuality.name() });
		assertTrue("Annual Load requested if water quality data type requested without any QW data type filters", ServiceParameterUtils.isAnnualLoadsRequested(params));

		params.put(DownloadServiceParameters.qwDataType.name(), new String[] { DownloadType.monthlyLoad.name() });
		assertFalse("Annual Load NOT requested if water quality data type requested with a QW data type filters that do not include Annual Load", ServiceParameterUtils.isAnnualLoadsRequested(params));

		params.put(DownloadServiceParameters.qwDataType.name(), new String[] { DownloadType.annualLoad.name(), DownloadType.discreteQw.name() });
		assertTrue("Annual Load requested if water quality data type requested with a QW data type filters that include Annual Load", ServiceParameterUtils.isAnnualLoadsRequested(params));
	}
	
	@Test
	public void testIsMonthlyLoadsRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Monthly not requested if params not provided", ServiceParameterUtils.isMonthlyLoadsRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.waterQuality.name() });
		assertTrue("Monthly Load requested if water quality data type requested without any QW data type filters", ServiceParameterUtils.isMonthlyLoadsRequested(params));

		params.put(DownloadServiceParameters.qwDataType.name(), new String[] { DownloadType.annualLoad.name() });
		assertFalse("Monthly Load NOT requested if water quality data type requested with a QW data type filters that do not include Monthly Load", ServiceParameterUtils.isMonthlyLoadsRequested(params));

		params.put(DownloadServiceParameters.qwDataType.name(), new String[] { DownloadType.monthlyLoad.name(), DownloadType.discreteQw.name() });
		assertTrue("Monthly Load requested if water quality data type requested with a QW data type filters that include Monthly Load", ServiceParameterUtils.isMonthlyLoadsRequested(params));
	}

	@Test
	public void testIsAnnualFlowRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Annual Flow not requested if params not provided", ServiceParameterUtils.isAnnualFlowRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.streamFlow.name() });
		assertTrue("Annual Flow requested if stream flow data type requested without any stream flow type filters", ServiceParameterUtils.isAnnualFlowRequested(params));

		params.put(DownloadServiceParameters.streamFlowType.name(), new String[] { DownloadType.monthlyFlow.name() });
		assertFalse("Annual Flow NOT requested if stream flow data type requested with a stream flow type filters that do not include Annual Flow", ServiceParameterUtils.isAnnualFlowRequested(params));

		params.put(DownloadServiceParameters.streamFlowType.name(), new String[] { DownloadType.annualFlow.name(), DownloadType.monthlyFlow.name() });
		assertTrue("Annual Flow requested if stream flow data type requested with a stream flow filters that include Annual Flow", ServiceParameterUtils.isAnnualFlowRequested(params));
	}

	@Test
	public void testIsMonthlyFlowRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Monthly Flow not requested if params not provided", ServiceParameterUtils.isMonthlyFlowRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.streamFlow.name() });
		assertTrue("Monthly Flow requested if stream flow data type requested without any stream flow type filters", ServiceParameterUtils.isMonthlyFlowRequested(params));

		params.put(DownloadServiceParameters.streamFlowType.name(), new String[] { DownloadType.annualFlow.name() });
		assertFalse("Monthly Flow NOT requested if stream flow data type requested with a stream flow type filters that do not include Monthly Flow", ServiceParameterUtils.isMonthlyFlowRequested(params));

		params.put(DownloadServiceParameters.streamFlowType.name(), new String[] { DownloadType.annualFlow.name(), DownloadType.monthlyFlow.name() });
		assertTrue("Monthly Flow requested if stream flow data type requested with a stream flow filters that include Monthly Flow", ServiceParameterUtils.isMonthlyFlowRequested(params));
	}

	@Test
	public void testIsDailyFlowRequested() {
		HashMap<String, String[]> params = new HashMap<>();
		
		assertFalse("Daily Flow not requested if params not provided", ServiceParameterUtils.isDailyFlowRequested(params));
		
		params.put(DownloadServiceParameters.dataType.name(), new String[] { DataType.streamFlow.name() });
		assertTrue("Daily Flow requested if stream flow data type requested without any stream flow type filters", ServiceParameterUtils.isDailyFlowRequested(params));

		params.put(DownloadServiceParameters.streamFlowType.name(), new String[] { DownloadType.annualFlow.name() });
		assertFalse("Daily Flow NOT requested if stream flow data type requested with a stream flow type filters that do not include Daily Flow", ServiceParameterUtils.isDailyFlowRequested(params));

		params.put(DownloadServiceParameters.streamFlowType.name(), new String[] { DownloadType.annualFlow.name(), DownloadType.dailyFlow.name() });
		assertTrue("Daily Flow requested if stream flow data type requested with a stream flow filters that include Daily Flow", ServiceParameterUtils.isDailyFlowRequested(params));
	}
}
