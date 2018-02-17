package it_fr.sap_un.osg.scheduler.n;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import it_fr.sap_un.osg.estimator.Estimator;
import it_fr.sap_un.osg.estimator.LearnerCorrectionReply;
import it_fr.sap_un.osg.estimator.LearnerEstimatorUpdate;
import it_fr.sap_un.osg.estimator.LearnerFeedback;
import it_fr.sap_un.osg.scheduler.IScheduler;
import it_fr.sap_un.osg.tuple.IAttributeHasher;
import it_fr.sap_un.osg.tuple.ITuple;
import it_fr.sap_un.osg.tuple.SimpleHasher;

public class Scheduler implements IScheduler {

	public final int k;
	private final IAttributeHasher hasher;
	public final int attributeId;

	private final int windowSize;

	private final FSM fsm;

	public Scheduler(int attributeId, int k, int windowSize) {
		this(new SimpleHasher(), attributeId, k, windowSize);
	}

	public Scheduler(IAttributeHasher hasher, int attributeId, int k,
			int windowSize) {
		super();
		System.out.println(this.getClass().getName());
		this.k = k;
		this.hasher = hasher;
		this.attributeId = attributeId;

		this.windowSize = windowSize * k;

		this.fsm = new FSM();

	}

	@Override
	public int getInstance(ITuple tuple) {
		return this.fsm.newTuple(tuple);
	}

	public void pushCorrection(int opId, int epoch, long correction) {
		try {
			this.fsm.feedBackQueue.put(new LearnerCorrectionReply(opId, epoch,
					correction));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void pushUpdate(int opId, int epoch, Estimator countMin) {
		try {
			this.fsm.feedBackQueue.put(new LearnerEstimatorUpdate(opId, epoch,
					countMin));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public class FSM {

		private long initTS = -1;

		private int m = 0;

		private final long[] estimatedTerminationTss;

		private final LinkedBlockingQueue<LearnerFeedback> feedBackQueue;

		private final Estimator[] estimator;

		private final LearnerCorrectionReply corrections[];

		private int epoch = 0;
		private int m_prime = 0;

		private State currentState = null;

		public FSM() {

			this.estimatedTerminationTss = new long[k];

			this.feedBackQueue = new LinkedBlockingQueue<LearnerFeedback>();

			this.estimator = new Estimator[k];
			Arrays.fill(this.estimator, null);

			this.corrections = new LearnerCorrectionReply[k];
			Arrays.fill(this.corrections, null);

		}

		public int newTuple(ITuple tuple) {

			State prevState = this.currentState;

			if (this.currentState != null) {
				this.currentState = this.currentState.newTuple(tuple);
			} else {
				this.currentState = new RoundRobin();
			}

			if (prevState != this.currentState)
				System.out.println(currentState);

			int id = currentState.getInstance(tuple);

			this.m++;

			return id;

		}

		private int lessLoaded() {
			long currTS = (System.nanoTime() - initTS) / 1000;
			int id = -1;
			long minLoad = Long.MAX_VALUE;
			for (int i = 0; i < estimatedTerminationTss.length; i++) {
				if (estimatedTerminationTss[i] < currTS) {
					// estimatedTerminationTss[i] = currTS;
				}

				if (estimatedTerminationTss[i] < minLoad) {
					id = i;
					minLoad = estimatedTerminationTss[i];
				}
			}

			return id;
		}

		long sumA = 0;

		public void updateLoad(ITuple tuple, int opId) {
			int value = Scheduler.this.hasher.hash(tuple.getValues().get(
					attributeId));
			long load = (long) this.estimator[opId].getAverageLatency(value);
			if (opId == 0) {
				sumA += load;
			}

			/*
			 * if (opId == 0) { sum += tuple.getLoad(opId); sum2 += (long)
			 * this.estimator[opId].getAverageLatency(value);
			 * System.out.println(sum + " VS " + sum2); }
			 * System.out.println(String.format(
			 * "[%03d] load: %06d, est: %06d, error; %06d (%f) ", value,
			 * tuple.getLoad(opId), load, load - tuple.getLoad(opId), (load -
			 * tuple.getLoad(opId)) / ((double) tuple.getLoad(opId))));
			 */
			this.estimatedTerminationTss[opId] += load;
		}

		public boolean rcvdUpdate(LearnerEstimatorUpdate update) {

			System.out.println(String.format("Scheduler [%d::%d] => %s",
					FSM.this.m, FSM.this.epoch, update));

			FSM.this.estimator[update.opId] = update.updatedCountMin;
			return true;
		}

		public boolean rcvdReply(LearnerCorrectionReply reply) {
			System.out.println(String.format("Scheduler [%d::%d] => %s",
					FSM.this.m, FSM.this.epoch, reply));
			System.out.println("sumA = " + sumA);
			if (reply.epoch == FSM.this.epoch) {
				if (FSM.this.corrections[reply.opId] == null) {
					FSM.this.corrections[reply.opId] = reply;

					boolean check = true;
					for (int i = 0; i < FSM.this.corrections.length; i++) {
						check &= corrections[i] != null;
					}

					if (check) {
						System.out.println("Correct All");
						for (int i = 0; i < FSM.this.corrections.length; i++) {

							System.out
									.println(String
											.format("Scheduler [%d::%d] => %s",
													FSM.this.m,
													FSM.this.epoch,
													estimatedTerminationTss[i]
															+ " + "
															+ FSM.this.corrections[i].correction
															+ " = "
															+ (FSM.this.estimatedTerminationTss[i] + FSM.this.corrections[i].correction)

															+ " (error: "
															+ (FSM.this.corrections[i].correction / ((double) FSM.this.estimatedTerminationTss[i]))
															+ ")"

											));

							FSM.this.estimatedTerminationTss[i] += FSM.this.corrections[i].correction;
						}
						FSM.this.m_prime = 0;
						return true;
					}

				} else {
					throw new RuntimeException();
				}

			} else if (reply.epoch >= FSM.this.epoch) {
				throw new RuntimeException();
			} else {
				// System.out.println(String.format("Old Epoch: %s => %d",
				// reply,
				// FSM.this.epoch));
			}
			return false;
		}

		private int getInstance(ITuple tuple) {
			int opId = this.lessLoaded();

			updateLoad(tuple, opId);

			return opId;
		}

		private int getInstance() {
			int opId = FSM.this.m % Scheduler.this.k;

			// FSM.this.estimatedTerminationTss[opId] = (System.nanoTime() -
			// initTS) / 1000;

			return opId;
		}

		private class Run extends State {

			@Override
			public State newTuple(ITuple tuple) {
				FSM.this.m_prime++;

				while (!feedBackQueue.isEmpty()) {
					LearnerFeedback feedBack = feedBackQueue.poll();
					if (feedBack instanceof LearnerEstimatorUpdate) {

						LearnerEstimatorUpdate update = (LearnerEstimatorUpdate) feedBack;
						if (FSM.this.rcvdUpdate(update)) {
							return new SendAll();
						}

					} else {
						throw new RuntimeException();
					}

				}

				if (FSM.this.m_prime >= Scheduler.this.windowSize) {
					return new SendAll();
				}

				return this;

			}

			@Override
			public int getInstance(ITuple tuple) {
				return FSM.this.getInstance(tuple);
			}
		}

		private class WaitAll extends State {
			long sum = 0;

			@Override
			public State newTuple(ITuple tuple) {

				while (!feedBackQueue.isEmpty()) {
					LearnerFeedback feedBack = feedBackQueue.poll();
					if (feedBack instanceof LearnerEstimatorUpdate) {

						LearnerEstimatorUpdate update = (LearnerEstimatorUpdate) feedBack;
						if (FSM.this.rcvdUpdate(update)) {
							return new SendAll();
						}

					} else if (feedBack instanceof LearnerCorrectionReply) {

						LearnerCorrectionReply reply = (LearnerCorrectionReply) feedBack;
						if (FSM.this.rcvdReply(reply)) {
							System.out.println(sum);
							return new Run();
						}
					}
				}
				return this;
			}

			@Override
			public int getInstance(ITuple tuple) {
				int id = FSM.this.getInstance(tuple);
				int value = Scheduler.this.hasher.hash(tuple.getValues().get(
						attributeId));
				long load = (long) FSM.this.estimator[id]
						.getAverageLatency(value);
				sum += load;
				return id;
			}
		}

		private class SendAll extends State {

			int currK = 0;

			public SendAll() {
				FSM.this.epoch++;
				Arrays.fill(FSM.this.corrections, null);
			}

			@Override
			public State newTuple(ITuple tuple) {

				if (currK >= k) {
					return new WaitAll();
				}

				while (!feedBackQueue.isEmpty()) {
					LearnerFeedback feedBack = feedBackQueue.poll();
					if (feedBack instanceof LearnerEstimatorUpdate) {

						LearnerEstimatorUpdate update = (LearnerEstimatorUpdate) feedBack;
						if (FSM.this.rcvdUpdate(update)) {
							return new SendAll();
						}

					} else if (feedBack instanceof LearnerCorrectionReply) {
						// Ignore
						/*
						 * LearnerCorrectionReply reply =
						 * (LearnerCorrectionReply) feedBack; if
						 * (rcvdReply(reply)) { return new Run(); }
						 */
					}

				}
				return this;
			}

			@Override
			public int getInstance(ITuple tuple) {
				int opId = currK++;
				updateLoad(tuple, opId);
				System.out.println(String.format(
						"estimatedTerminationTss[%d]: %d", opId,
						estimatedTerminationTss[opId]));
				SchedulerCorrectionRequest data = new SchedulerCorrectionRequest(
						estimatedTerminationTss[opId], FSM.this.epoch);
				tuple.setSchedulerData(data);

				return opId;
			}

		}

		private class RoundRobin extends State {

			@Override
			public State newTuple(ITuple tuple) {

				if (FSM.this.initTS < 0) {
					FSM.this.initTS = System.nanoTime();
				}

				while (!feedBackQueue.isEmpty()) {
					LearnerFeedback feedBack = feedBackQueue.poll();
					if (feedBack instanceof LearnerEstimatorUpdate) {

						LearnerEstimatorUpdate update = (LearnerEstimatorUpdate) feedBack;

						System.out.println(String.format(
								"Scheduler [%d::%d] => %s", FSM.this.m,
								FSM.this.epoch, update));

						FSM.this.estimator[update.opId] = update.updatedCountMin;

						boolean check = true;
						for (int i = 0; i < FSM.this.estimator.length; i++) {
							check &= estimator[i] != null;
						}
						if (check) {
							return new SendAll();
						}

					} else {
						throw new RuntimeException();
					}

				}
				return this;
			}

			@Override
			public int getInstance(ITuple tuple) {
				return FSM.this.getInstance();
			}
		}

		private abstract class State {

			abstract public State newTuple(ITuple tuple);

			abstract public int getInstance(ITuple tuple);

			@Override
			public String toString() {
				return String.format("Scheduler [%d::%d] => %s", FSM.this.m,
						FSM.this.epoch, this.getClass().getSimpleName());

			}
		}

	}

}
