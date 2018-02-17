package it_fr.sap_un.osg.estimator;

import java.nio.ByteBuffer;

public class SnapShot {

	public final int t;
	public final int k;

	final long[][] avgLatencies;

	public SnapShot(Estimator estimator) {

		this.t = estimator.t;
		this.k = estimator.k;
		this.avgLatencies = new long[estimator.t][estimator.k];

		for (int i = 0; i < estimator.t; i++) {
			for (int j = 0; j < estimator.k; j++) {
				this.avgLatencies[i][j] = (long) Math
						.round(((double) estimator.latency[i][j])
								/ ((double) estimator.count[i][j]));
			}
		}
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putInt(this.t);
		buffer.putInt(this.k);
		for (int i = 0; i < this.t; i++) {
			for (int j = 0; j < this.k; j++) {
				buffer.putLong(avgLatencies[i][j]);
			}
		}
	}

	public static SnapShot deSerialize(ByteBuffer buffer) {
		return new SnapShot(buffer);
	}

	private SnapShot(ByteBuffer buffer) {
		this.t = buffer.getInt();
		this.k = buffer.getInt();

		this.avgLatencies = new long[t][k];

		for (int i = 0; i < t; i++) {
			for (int j = 0; j < k; j++) {
				this.avgLatencies[i][j] = buffer.getLong();
			}
		}
		for (int i = 0; i < this.t; i++) {
			for (int j = 0; j < this.k; j++) {
				avgLatencies[i][j] = buffer.getLong();
			}
		}

	}
}