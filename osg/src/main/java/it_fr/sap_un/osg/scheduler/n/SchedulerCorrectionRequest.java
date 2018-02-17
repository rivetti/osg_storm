package it_fr.sap_un.osg.scheduler.n;

public class SchedulerCorrectionRequest {
	public final long estimatedTerminationTs;
	public final int epoch;

	public SchedulerCorrectionRequest(long estimatedTerminationTS, int epoch) {
		super();
		this.estimatedTerminationTs = estimatedTerminationTS;
		this.epoch = epoch;
	}

	@Override
	public String toString() {
		return "SchedulerCorrectionRequest [estimatedTerminationTS="
				+ estimatedTerminationTs + ", epoch=" + epoch + "]";
	}

}