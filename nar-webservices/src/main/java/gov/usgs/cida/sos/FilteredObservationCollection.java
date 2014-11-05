package gov.usgs.cida.sos;

import javax.xml.stream.XMLStreamReader;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class FilteredObservationCollection extends ObservationCollection {
	
	private final String procedure;
	private final String observedProperty;
	private final String featureOfInterest;

	public FilteredObservationCollection(XMLStreamReader reader, String procedure,
			String observedProperty, String featureOfInterest) {
		super(reader);
		this.procedure = procedure;
		this.observedProperty = observedProperty;
		this.featureOfInterest = featureOfInterest;
	}

	@Override
	public Observation next() {
		return super.next();
	}

	@Override
	public boolean hasNext() {
		boolean filteredNext = false;
		
		while (super.hasNext() && !filteredNext) {
			if (filter()) {
				filteredNext = true;
			} else {
				currentObservation.setReady(false);
			}
		}
		return filteredNext;
	}
	
	private boolean filter() {
		boolean allEqual = true;
		if (currentObservation.metadata() != null) {
			if (this.procedure != null && !this.procedure.equals(currentObservation.metadata().procedure())) {
				allEqual = false;
			}
			if (this.observedProperty != null && !this.observedProperty.equals(currentObservation.metadata().observedProperty())) {
				allEqual = false;
			}
			if (this.featureOfInterest != null && !this.featureOfInterest.equals(currentObservation.metadata().featureOfInterest())) {
				allEqual = false;
			}
		}
		return allEqual;
	}

}
