package it_fr.sap_un.osg.estimator;

import org.apache.commons.math3.random.RandomDataGenerator;

import it_fr.sap_un.osg.common.hash.CWHashFunctions;

public class Estimator implements IEstimator {

	public final int k;
	public final int t;

	final long[][] count;
	final long[][] latency;
	private final long[] as;
	private final long[] bs;
	private final long prime = 10000019L;
	private final CWHashFunctions[] hashes;

	public Estimator(double epsilon, double delta, long seed) {
		this((int) Math.ceil(Math.log(1.0 / delta) / Math.log(2)), (int) Math
				.ceil(Math.E / epsilon), seed);
	}

	public Estimator(int t, int k, long seed) {
		super();
		this.k = k;
		this.t = t;

		System.out.println(String.format("k %d t %d", k, t));

		this.count = new long[t][k];
		this.latency = new long[t][k];
		this.hashes = new CWHashFunctions[t];

		RandomDataGenerator uniformGenerator = new RandomDataGenerator();
		uniformGenerator.reSeed(seed);

		this.as = new long[t];
		this.bs = new long[t];
		for (int i = 0; i < hashes.length; i++) {
			this.as[i] = uniformGenerator.nextLong(1, prime - 1);
			this.bs[i] = uniformGenerator.nextLong(1, prime - 1);
			hashes[i] = new CWHashFunctions(k, prime, this.as[i], this.bs[i]);
		}
	}

	public void newSample(int value, long latency) {
		for (int i = 0; i < t; i++) {
			int hash = hashes[i].hash(value);
			this.count[i][hash]++;
			this.latency[i][hash] += latency;
		}
	}

	public long getAverageLatency(int value) {
		long min = Integer.MAX_VALUE;
		long avgLatency = 0;
		for (int i = 0; i < t; i++) {
			int hash = hashes[i].hash(value);
			long freq = count[i][hash];
			long lat = latency[i][hash];

			if (min > freq) {
				min = freq;
				avgLatency = (long) Math
						.round(((double) lat) / ((double) freq));
			}
		}
		return avgLatency;
	}

	public SnapShot getSnapShot() {
		return new SnapShot(this);
	}

	public double error(SnapShot snapshot) {
		double error = 0.0;
		long thisAvgLatencyTotal = 0;
		for (int i = 0; i < t; i++) {

			for (int j = 0; j < as.length; j++) {
				long thisAvgLatency = (long) Math
						.round(((double) this.latency[i][j])
								/ ((double) this.count[i][j]));

				long thatAvgLatency = snapshot.avgLatencies[i][j];

				thisAvgLatencyTotal += thisAvgLatency;
				error += Math.abs(thisAvgLatency - thatAvgLatency);
			}
		}

		error = error / ((double) thisAvgLatencyTotal);

		return error;
	}
}