package gov.usgs.cida.nar.service;

public enum DownloadType {
	annualLoad("Annual loads"),
	annualFlow("Annual flow"),
	mayLoad("May loads"),
	dailyFlow("Daily flow"),
	discreteQw("Discrete QW"),
	siteAttribute("Site attribute");
	
	private String title;
	DownloadType(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
}
