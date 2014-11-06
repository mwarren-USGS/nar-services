package gov.usgs.cida.sos;

import javax.xml.stream.XMLStreamReader;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class FilteredObservationCollection extends ObservationCollection {
	
	private final OrderedFilter filter;

	public FilteredObservationCollection(XMLStreamReader reader, OrderedFilter filter) {
		super(reader);
		this.filter = filter;
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
			if (this.filter.procedure != null && !this.filter.procedure.equals(currentObservation.metadata().procedure())) {
				allEqual = false;
			}
			if (this.filter.observedProperty != null && !this.filter.observedProperty.equals(currentObservation.metadata().observedProperty())) {
				allEqual = false;
			}
			if (this.filter.featureOfInterest != null && !this.filter.featureOfInterest.equals(currentObservation.metadata().featureOfInterest())) {
				allEqual = false;
			}
		}
		return allEqual;
	}

}
