package it_fr.sap_un.osg.tuple;


public class SimpleHasher implements IAttributeHasher {

	@Override
	public int hash(Object attributeValue) {
		return (int) attributeValue;
	}

}