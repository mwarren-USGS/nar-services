package gov.usgs.cida.nar.service;

public enum DownloadServiceParameters {
	mimeType("Format"),
	dataType("Data type"),
	qwDataType("Water Quality Data type"),
	streamFlowType("Stream Flow Time Series"),
	constituent("Constituent"),
	siteType("Site Type"),
	stationId("Station ID"),
	state("State"),
	startDateTime("Start Date/Time"),
	endDateTime("End Date/Time")
	;
	
	//String constants so we can use them in annotations, SHOULD MATCH ENUM NAME above
	public static final String MIME_TYPE_PARAM = "mimeType";
	public static final String DATA_TYPE_PARAM = "dataType";
	public static final String QW_DATA_TYPE_PARAM = "qwDataType";
	public static final String STREAM_FLOW_TYPE_PARAM = "streamFlowType";
	public static final String CONSTITUENT_PARAM = "constituent";
	public static final String SITE_TYPE_PARAM = "siteType";
	public static final String STATION_ID_PARAM = "stationId";
	public static final String STATE_PARAM = "state";
	public static final String START_DATE_PARAM = "startDateTime";
	public static final String END_DATE_PARAM = "endDateTime";
	
	private String title;
	DownloadServiceParameters(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
}
