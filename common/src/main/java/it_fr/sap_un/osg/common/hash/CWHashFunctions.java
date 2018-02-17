package it_fr.sap_un.osg.common.hash;

public class CWHashFunctions {

	public final long codomainSize;
	public final long prime;
	public final long a;
	public final long b;

	public CWHashFunctions(int codomainSize, long prime, long a, long b) {
		super();
		this.codomainSize = codomainSize;
		this.prime = prime;
		this.a = a;
		this.b = b;
	}

	public static CWHashFunctions getCWHashFunctions(int codomainSize, long a,
			long b) {
		return new CWHashFunctions(codomainSize, 10000019l, a, b);
	}

	public int hash(int value) {
		long longValue = (long) value;
		return (int) Math.abs((int) ((int) ((a * longValue + b) % prime))
				% codomainSize);
	}

	public int hash(double value) {
		long longValue = (long) value;
		return (int) Math.abs((int) ((int) ((a * longValue + b) % prime))
				% codomainSize);
	}

}
