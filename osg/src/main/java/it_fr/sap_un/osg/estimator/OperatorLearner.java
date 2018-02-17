package it_fr.sap_un.osg.estimator;

import it_fr.sap_un.osg.scheduler.n.Scheduler;
import it_fr.sap_un.osg.scheduler.n.SchedulerCorrectionRequest;
import it_fr.sap_un.osg.tuple.IAttributeHasher;
import it_fr.sap_un.osg.tuple.ITuple;
import it_fr.sap_un.osg.tuple.SimpleHasher;

public class OperatorLearner implements IOperatorLearner {

	private final int opId;
	private final IAttributeHasher hasher;
	private final int attributeId;

	private final FSM fsm;

	private final double delta;
	private final double epsilon;
	private final long seed;

	private final long windowSize;

	private final Scheduler scheduler;

	public OperatorLearner(int opId, double delta, double epsilon, long seed,
			int attributeId, long windowSize, Scheduler scheduler) {
		this(opId, delta, epsilon, seed, new SimpleHasher(), attributeId,
				windowSize, scheduler);
	}

	public OperatorLearner(int opId, double delta, double epsilon, long seed,
			IAttributeHasher hasher, int attributeId, long windowSize,
			Scheduler scheduler) {
		super();
		System.out.println(this.getClass().getName());

		this.opId = opId;

		this.delta = delta;
		this.epsilon = epsilon;
		this.seed = seed;

		this.hasher = hasher;
		this.attributeId = attributeId;

		this.fsm = new FSM();

		this.windowSize = windowSize;

		this.scheduler = scheduler;

	}

	@Override
	public void newTuple(ITuple tuple, long latency) {

		this.fsm.newTuple(tuple, latency / 1000);
	}

	private class FSM {

		public final double TOLLERANCE = 0.05;

		private Estimator estimator;
		private SnapShot snaphot;

		private long initTS = -1;

		private int window = 0;

		private int m = 0;

		private State currentState = null;

		public void newTuple(ITuple tuple, long latency) {

			// Update FSM
			State prevState = this.currentState;

			if (this.currentState != null) {
				this.currentState = this.currentState.newTuple(tuple, latency);
			} else {
				this.currentState = new Start();
			}

			if (prevState != this.currentState)
				System.out.println(currentState);

			// Update Estimator
			int value = OperatorLearner.this.hasher.hash(tuple.getValues().get(
					OperatorLearner.this.attributeId));

			this.estimator.newSample(value, latency);

			// Reply to correction requests
			SchedulerCorrectionRequest data = (SchedulerCorrectionRequest) tuple
					.getSchedulerData();

			if (data != null && data.estimatedTerminationTs > 0) {

				long correction = ((System.nanoTime() - FSM.this.initTS) / 1000)
						- data.estimatedTerminationTs;
				// System.out.println(String.format("sched: %d, op: %d",
				// data.estimatedTerminationTs,
				// (System.nanoTime() - FSM.this.initTS) / 1000));

				scheduler.pushCorrection(opId, data.epoch, correction);
			}
			this.m++;

		}

		private class Stabilizing extends State {

			@Override
			public State newTuple(ITuple tuple, long latency) {

				if (m % OperatorLearner.this.windowSize == 0) {

					FSM.this.window++;
					double error = FSM.this.estimator.error(FSM.this.snaphot);
					if (error <= TOLLERANCE) {

						System.out
								.println(String
										.format("[%02d] OperatorLearner [%d::%d] Estimator Sabilized [%f <= %f ]",
												opId, FSM.this.m,
												FSM.this.window, error,
												TOLLERANCE));
						scheduler.pushUpdate(opId, FSM.this.window,
								FSM.this.estimator);

						return new Start();
					} else {
						System.out
								.println(String
										.format("[%02d] OperatorLearner [%d::%d] Estimator NOT Sabilized [%f > %f ]",
												opId, FSM.this.m,
												FSM.this.window, error,
												TOLLERANCE));

						FSM.this.snaphot = FSM.this.estimator.getSnapShot();
						return this;
					}

				}

				return this;

			}
		}

		private class Start extends State {

			public Start() {
				FSM.this.estimator = new Estimator(epsilon, delta, seed);
				FSM.this.snaphot = null;
			}

			@Override
			public State newTuple(ITuple tuple, long latency) {
				if (FSM.this.initTS < 0) {
					FSM.this.initTS = System.nanoTime();
				}

				if (m % OperatorLearner.this.windowSize == 0) {

					FSM.this.window++;

					FSM.this.snaphot = FSM.this.estimator.getSnapShot();

					return new Stabilizing();

				}

				return this;
			}

		}

		private abstract class State {

			abstract public State newTuple(ITuple tuple, long latency);

			@Override
			public String toString() {
				return String.format("[%02d] OperatorLearner [%d::%d] => %s",
						opId, FSM.this.m, FSM.this.window, this.getClass()
								.getSimpleName());

			}
		}
	}

}
