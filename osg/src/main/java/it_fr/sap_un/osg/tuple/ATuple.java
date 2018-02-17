package it_fr.sap_un.osg.tuple;

import java.util.ArrayList;

public abstract class ATuple implements ITuple {

	private static int currId = 0;

	public final int id;
	private final ArrayList<Object> values;

	private Object schedulerData = null;

	public ATuple(ArrayList<Object> values) {
		super();
		this.values = values;
		this.id = currId++;
	}

	@Override
	public ArrayList<Object> getValues() {
		return this.values;
	}

	@Override
	public void setSchedulerData(Object data) {
		this.schedulerData = data;

	}

	@Override
	public Object getSchedulerData() {
		return this.schedulerData;
	}

}
