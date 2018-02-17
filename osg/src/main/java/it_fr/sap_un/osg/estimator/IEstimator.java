package it_fr.sap_un.osg.estimator;


public interface IEstimator {

	public void newSample(int value, long latency);

	public long getAverageLatency(int value);

}