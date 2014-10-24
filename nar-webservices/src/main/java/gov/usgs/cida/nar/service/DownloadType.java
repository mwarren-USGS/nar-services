package gov.usgs.cida.nar.service;

public enum DownloadType {
	annualLoads("Annual loads"),
	annualFlow("Annual flow"),
	monthlyLoads("Monthly loads"),
	monthlyFlow("Monthly flow"),
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
