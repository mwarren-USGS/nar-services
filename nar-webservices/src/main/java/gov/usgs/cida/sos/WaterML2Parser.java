package gov.usgs.cida.sos;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class WaterML2Parser {
	
	private File observationResultFile;
	
	public WaterML2Parser(File observationResultFile) {
		this.observationResultFile = observationResultFile;
	}
	
	public ObservationCollection getObservations() throws FileNotFoundException, XMLStreamException {
		FileReader reader = new FileReader(observationResultFile);
		XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(reader);
		ObservationCollection observationCollection = new ObservationCollection(xmlReader);
		return observationCollection;
	}
	
}
