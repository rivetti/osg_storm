package it_fr.sap_un.osg.scheduler.n;

import java.util.Arrays;

import it_fr.sap_un.osg.estimator.IEstimator;
import it_fr.sap_un.osg.scheduler.IScheduler;
import it_fr.sap_un.osg.tuple.IAttributeHasher;
import it_fr.sap_un.osg.tuple.ITuple;
import it_fr.sap_un.osg.tuple.SimpleHasher;

public class OfflineSchedulerWO implements IScheduler {

	private long initTS = -1;

	public final int k;
	private final IAttributeHasher hasher;
	public final int attributeId;

	private final long[] estimatedTerminationTss;

	private final IEstimator[] estimator;

	public OfflineSchedulerWO(int attributeId, int k, IEstimator[] estimator) {
		this(new SimpleHasher(), attributeId, k, estimator);
	}

	public OfflineSchedulerWO(IAttributeHasher hasher, int attributeId, int k,
			IEstimator[] estimator) {
		super();
		System.out.println(this.getClass().getName());
		this.k = k;
		this.hasher = hasher;
		this.attributeId = attributeId;

		this.estimatedTerminationTss = new long[k];

		this.estimator = estimator;

	}

	@Override
	public int getInstance(ITuple tuple) {

		if (this.initTS < 0) {
			this.initTS = System.nanoTime();
		}

		int opId = this.lessLoaded();

		int value = this.hasher.hash(tuple.getValues().get(attributeId));

		long load = (long) this.estimator[opId].getAverageLatency(value);

		this.estimatedTerminationTss[opId] += load;

		return opId;

	}

	private int lessLoaded() {
		long currTS = (System.nanoTime() - initTS) / 1000;
		int id = -1;
		long minLoad = Long.MAX_VALUE;
		for (int i = 0; i < estimatedTerminationTss.length; i++) {
			if (estimatedTerminationTss[i] < currTS) {
				estimatedTerminationTss[i] = currTS;
			}

			if (estimatedTerminationTss[i] < minLoad) {
				id = i;
				minLoad = estimatedTerminationTss[i];
			}
		}

		return id;
	}

}
