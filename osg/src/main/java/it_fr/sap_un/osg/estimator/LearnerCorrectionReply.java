package it_fr.sap_un.osg.estimator;

public class LearnerCorrectionReply extends LearnerFeedback {
	public final long correction;
	public final int epoch;

	public LearnerCorrectionReply(int opId, int epoch, long correction) {
		super(opId);
		this.epoch = epoch;
		this.correction = correction;
	}

	@Override
	public String toString() {
		return "LearnerCorrectionReply [correction=" + correction + ", epoch="
				+ epoch + ", opId=" + opId + "]";
	}

}