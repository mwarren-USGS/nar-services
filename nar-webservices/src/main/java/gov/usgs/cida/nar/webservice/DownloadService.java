package gov.usgs.cida.nar.webservice;

import gov.usgs.cida.nar.service.DiscreteQwService;
import gov.usgs.cida.nar.service.DownloadServiceParameters;

import static gov.usgs.cida.nar.service.DownloadServiceParameters.*;

import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nar.service.SiteInformationService;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;
import gov.usgs.cida.nar.util.ServiceParameterUtils;
import gov.usgs.webservices.framework.basic.MimeType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.naming.NamingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("download")
public class DownloadService {
	private final static Logger LOG = LoggerFactory.getLogger(DownloadService.class);
	
	@GET
	@Path("/bundle/zip")
	@Produces("application/zip")
	public Response downloadZippedBundle(
			@QueryParam(MIME_TYPE_PARAM) final String mimeTypeParam,
			@QueryParam(DATA_TYPE_PARAM) final List<String> dataType,
			@QueryParam(QW_DATA_TYPE_PARAM) final List<String> qwDataType,
			@QueryParam(STREAM_FLOW_TYPE_PARAM) final List<String> streamFlowType,
			@QueryParam(CONSTITUENT_PARAM) final List<String> constituent,
			@QueryParam(SITE_TYPE_PARAM) final List<String> siteType,
			@QueryParam(STATION_ID_PARAM) final List<String> stationId,
			@QueryParam(STATE_PARAM) final List<String> state,
			@QueryParam(START_DATE_PARAM) final List<String> startDateTime,
			@QueryParam(END_DATE_PARAM) final List<String> endDateTime) throws NamingException {
		LOG.debug("Stream full zipped bundle started");
		
		final MimeType mimeType = MimeType.lookup(mimeTypeParam);
		if (mimeType == null) {
			throw new RuntimeException("mimeType not supported");
		}

		return Response.ok(new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				ZipOutputStream zip = null;
				try {
					zip = new ZipOutputStream(output, StandardCharsets.UTF_8);
					addRequestSummaryEntry(zip, buildRequestDescriptionFromParams(mimeType,
							dataType,
							qwDataType,
							streamFlowType,
							constituent,
							siteType,
							stationId,
							state,
							startDateTime,
							endDateTime));
					
					if(ServiceParameterUtils.isSiteInformationRequested(dataType)) {
						addSiteInformationEntry(zip, mimeType,
								siteType,
								stationId,
								state);
					}
					
					if(ServiceParameterUtils.isDiscreteQwRequested(dataType, qwDataType)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isAnnualLoadsRequested(dataType, qwDataType)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isMonthlyLoadsRequested(dataType, qwDataType)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isDailyFlowRequested(dataType, streamFlowType)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isAnnualFlowRequested(dataType, streamFlowType)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isMonthlyFlowRequested(dataType, streamFlowType)) {
						//TODO hook up
					}
				} finally {
						IOUtils.closeQuietly(zip);
				}
			}
		}).header("Content-Disposition", "attachment; filename=\"data.zip\"").build();
	}

	private void addSiteInformationEntry(ZipOutputStream zip, 
			final MimeType mimeType,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state
			) throws IOException {
		zip.putNextEntry(new ZipEntry(SiteInformationService.SITE_ATTRIBUTE_OUT_FILENAME + "." + mimeType.getFileSuffix()));
		new SiteInformationService().streamData(zip, 
				mimeType,
				siteType,
				stationId,
				state);
		zip.closeEntry();
	}
	
	private void addDiscreteQwEntry(ZipOutputStream zip,
			final MimeType mimeType,
			final List<String> qwDataType,
			final List<String> constituent,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state,
			final List<String> startDateTime,
			final List<String> endDateTime) throws IOException {
		zip.putNextEntry(new ZipEntry(DiscreteQwService.DISCRETE_QW_OUT_FILENAME + "." + mimeType.getFileSuffix()));
		new DiscreteQwService().streamData(zip, mimeType, qwDataType, constituent, siteType, stationId, state, startDateTime, endDateTime);
		zip.closeEntry();
	}
	
	private void addRequestSummaryEntry(ZipOutputStream zip, String requestDescription) throws IOException {
		zip.putNextEntry(new ZipEntry("request.txt"));
		zip.write(requestDescription.getBytes());
		zip.closeEntry();
	}
	
	private String buildRequestDescriptionFromParams(
			final MimeType mimeType,
			final List<String> dataType,
			final List<String> qwDataType,
			final List<String> streamFlowType,
			final List<String> constituent,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state,
			final List<String> startDateTime,
			final List<String> endDateTime) {
		StringBuffer sb = new StringBuffer();
		
		//List service criteria
		sb.append("Request criteria provided\n");
		prettyPrintParamList(sb, Arrays.asList(mimeType.name()), DownloadServiceParameters.MIME_TYPE_PARAM);
		prettyPrintParamList(sb, dataType, DownloadServiceParameters.DATA_TYPE_PARAM);
		prettyPrintParamList(sb, qwDataType, DownloadServiceParameters.QW_DATA_TYPE_PARAM);
		prettyPrintParamList(sb, streamFlowType, DownloadServiceParameters.STREAM_FLOW_TYPE_PARAM);
		prettyPrintParamList(sb, constituent, DownloadServiceParameters.CONSTITUENT_PARAM);
		prettyPrintParamList(sb, siteType, DownloadServiceParameters.SITE_TYPE_PARAM);
		prettyPrintParamList(sb, stationId, DownloadServiceParameters.STATION_ID_PARAM);
		prettyPrintParamList(sb, state, DownloadServiceParameters.STATE_PARAM);
		prettyPrintParamList(sb, startDateTime, DownloadServiceParameters.START_DATE_PARAM);
		prettyPrintParamList(sb, endDateTime, DownloadServiceParameters.END_DATE_PARAM);
		sb.append("\n\n");
		
		//List data headers which match request
		if(ServiceParameterUtils.isSiteInformationRequested(dataType)) {
			appendDataTypeDescription(sb, DownloadType.siteAttribute);
		}

		if(ServiceParameterUtils.isDiscreteQwRequested(dataType, qwDataType)) {
			appendDataTypeDescription(sb, DownloadType.discreteQw);
		}

		if(ServiceParameterUtils.isAnnualLoadsRequested(dataType, qwDataType)) {
			appendDataTypeDescription(sb, DownloadType.annualLoad);
		}

		if(ServiceParameterUtils.isMonthlyLoadsRequested(dataType, qwDataType)) {
			appendDataTypeDescription(sb, DownloadType.monthlyLoad);
		}

		if(ServiceParameterUtils.isDailyFlowRequested(dataType, streamFlowType)) {
			appendDataTypeDescription(sb, DownloadType.dailyFlow);
		}

		if(ServiceParameterUtils.isAnnualFlowRequested(dataType, streamFlowType)) {
			appendDataTypeDescription(sb, DownloadType.annualFlow);
		}

		if(ServiceParameterUtils.isMonthlyFlowRequested(dataType, streamFlowType)) {
			appendDataTypeDescription(sb, DownloadType.monthlyFlow);
		}
		
		return sb.toString();
	}
	
	private void appendDataTypeDescription(StringBuffer sb, DownloadType t) {
		sb.append(t.getTitle());
		sb.append("\n");
		sb.append(DescriptionLoaderSingleton.getDescription(t.getTitle()));
		sb.append("\n\n");
	}
	
	private void prettyPrintParamList(StringBuffer sb, List<String> values, String name) {
		if(values != null && values.size() > 0) {
			sb.append(" - ");
			sb.append(DownloadServiceParameters.valueOf(name).getTitle());
			sb.append(": ");
			for(String v : values) {
				sb.append(v);
				sb.append(", ");
			}
			sb.delete(sb.length() - 2, sb.length());
			sb.append("\n");
		}
	}
}
