package it_fr.sap_un.osg.estimator.offline;

import it_fr.sap_un.osg.estimator.IEstimator;
import it_fr.sap_un.osg.estimator.IOperatorLearner;
import it_fr.sap_un.osg.tuple.ITuple;

public interface IOfflineOperatorLearner extends IOperatorLearner {

	public IEstimator getEstimator();

}
