package it_fr.sap_un.osg.estimator.offline;

import it_fr.sap_un.osg.estimator.Estimator;
import it_fr.sap_un.osg.estimator.IEstimator;
import it_fr.sap_un.osg.tuple.IAttributeHasher;
import it_fr.sap_un.osg.tuple.ITuple;
import it_fr.sap_un.osg.tuple.SimpleHasher;

public class OffLineOperatorLearner implements IOfflineOperatorLearner {

	private final int opId;
	private final IAttributeHasher hasher;
	private final int attributeId;

	private final double delta;
	private final double epsilon;
	private final long seed;

	private final Estimator estimator;

	public OffLineOperatorLearner(int opId, double delta, double epsilon,
			long seed, int attributeId) {
		this(opId, delta, epsilon, seed, new SimpleHasher(), attributeId);
	}

	public OffLineOperatorLearner(int opId, double delta, double epsilon,
			long seed, IAttributeHasher hasher, int attributeId) {
		super();
		System.out.println(this.getClass().getName());

		this.opId = opId;

		this.delta = delta;
		this.epsilon = epsilon;
		this.seed = seed;

		this.hasher = hasher;
		this.attributeId = attributeId;

		this.estimator = new Estimator(epsilon, delta, seed);

	}

	@Override
	public void newTuple(ITuple tuple, long latency) {

		int value = OffLineOperatorLearner.this.hasher.hash(tuple.getValues()
				.get(OffLineOperatorLearner.this.attributeId));

		this.estimator.newSample(value, latency);

	}

	@Override
	public IEstimator getEstimator() {
		return this.estimator;
	}

}
