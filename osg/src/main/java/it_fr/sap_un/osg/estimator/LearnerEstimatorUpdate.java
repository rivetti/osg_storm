package it_fr.sap_un.osg.estimator;


public class LearnerEstimatorUpdate extends LearnerFeedback {

	public final Estimator updatedCountMin;
	public final int window;

	public LearnerEstimatorUpdate(int opId, int window,
			Estimator updatedCountMin) {
		super(opId);
		this.updatedCountMin = updatedCountMin;
		this.window = window;
	}

	@Override
	public String toString() {
		return "LearnerEstimatorUpdate [updatedCountMin=" + updatedCountMin
				+ ", window=" + window + ", opId=" + opId + "]";
	}

}