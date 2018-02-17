package it_fr.sap_un.osg.estimator;

import it_fr.sap_un.osg.tuple.ITuple;

public interface IOperatorLearner {

	public void newTuple(ITuple tuple, long latency);

}
