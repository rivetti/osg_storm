package it_fr.sap_un.osg.tuple;

import java.util.ArrayList;

public interface ITuple {

	public ArrayList<Object> getValues();

	public void setSchedulerData(Object data);

	public Object getSchedulerData();

	public long getLoad(int opId);

}
