package gov.usgs.cida.nar.service;

public enum DataType {
	streamFlow("Stream Flow "),
	waterQuality("Water Quality"),
	siteInformation("Site Information");
	
	private String title;
	DataType(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
}
