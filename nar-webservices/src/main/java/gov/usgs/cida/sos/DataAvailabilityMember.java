package gov.usgs.cida.sos;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.geotools.xlink.XLINK;

public class DataAvailabilityMember {
	private String procedure;
	private String observedProperty;
	private String featureOfInterest;

	//TODO add in phenomenomTime property

	public DataAvailabilityMember() {
	}
	
	public DataAvailabilityMember(String procedure, String observedProperty, String featureOfInterest) {
		this.procedure = procedure;
		this.observedProperty = observedProperty;
		this.featureOfInterest = featureOfInterest;
	}
	
	public String getProcedure() {
		return procedure;
	}
	public void setProcedure(String procedure) {
		this.procedure = procedure;
	}
	public String getObservedProperty() {
		return observedProperty;
	}
	public void setObservedProperty(String observedProperty) {
		this.observedProperty = observedProperty;
	}
	public String getFeatureOfInterest() {
		return featureOfInterest;
	}
	public void setFeatureOfInterest(String featureOfInterest) {
		this.featureOfInterest = featureOfInterest;
	}
	
	public static List<DataAvailabilityMember> buildListFromXmlInputStream(InputStream gdaResponseStream) throws EndOfXmlStreamException, XMLStreamException, FactoryConfigurationError {
		List<DataAvailabilityMember> dataAvailabilityMembers = new ArrayList<>();
		
		InputStreamReader reader = new InputStreamReader(gdaResponseStream);
		XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(reader);
		
		DataAvailabilityMember currentMember = null;
		while (xmlReader.hasNext()) {
			switch (xmlReader.next()) {
				case XMLStreamConstants.START_ELEMENT:
					if (isElement(DataAvailabilityMetadata.DATA_AVAILABILITY_MEMBER_ELEMENT, xmlReader)) {
						currentMember = new DataAvailabilityMember();
					}
					
					if (isElement(DataAvailabilityMetadata.PROCEDURE_ELEMENT, xmlReader)) {
						currentMember.setProcedure(getHrefAttribute(xmlReader));
					}
					if (isElement(DataAvailabilityMetadata.OBSERVED_PROPERTY_ELEMENT, xmlReader)) {
						currentMember.setObservedProperty(getHrefAttribute(xmlReader));
					}
					if (isElement(DataAvailabilityMetadata.FEATURE_OF_INTEREST_ELEMENT, xmlReader)) {
						currentMember.setFeatureOfInterest(getHrefAttribute(xmlReader));
					}
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (isElement(DataAvailabilityMetadata.DATA_AVAILABILITY_MEMBER_ELEMENT, xmlReader)) {
						dataAvailabilityMembers.add(currentMember);
					}
					break;
				case XMLStreamConstants.END_DOCUMENT:
					break;
			}
		}
		
		return dataAvailabilityMembers;
	}
	
	/**
	 * Returns true if current XML element is equal to that passed in
	 * @param elementName name to check for equality with element
	 * @return true if equal
	 */
	private static boolean isElement(String elementName, XMLStreamReader reader) {
		boolean isElement = false;
		if (elementName != null) {
			isElement = elementName.equals(reader.getLocalName());
		} 
		return isElement;
	}
	
	private static String getHrefAttribute(XMLStreamReader reader) {
		return reader.getAttributeValue(XLINK.HREF.getNamespaceURI(), XLINK.HREF.getLocalPart());
	}
	
	public static boolean contains(List<DataAvailabilityMember> members, DataAvailabilityMember member) {
		if(members != null) {
			for(DataAvailabilityMember da : members) {
				if(da.getProcedure().equals(member.getProcedure()) && 
						da.getObservedProperty().equals(member.getObservedProperty()) &&
						da.getFeatureOfInterest().equals(member.getFeatureOfInterest())) {
					return true;
				}
			}
		}
		return false;
	}
}
