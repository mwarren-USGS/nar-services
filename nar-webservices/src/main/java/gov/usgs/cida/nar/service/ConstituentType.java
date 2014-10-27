package gov.usgs.cida.nar.service;

public enum ConstituentType {
	nitrate("Nitrate"), 
	totalNitrogen("Total Nitrogen"), 
	totalPhosphorus("Total Phosphorus"), 
	suspendedSediment("Suspended Sediment");
	
	private String title;
	ConstituentType(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
}
