package com.gigasynapse.common;

public class Hash {	
	private static long ht[] = createTable();
	private static long[] createTable() {
		long ht[] = new long[256];
		long h = 0x544B2FBACAAF1684L;
		for (int i = 0; i < 256; i++) {
			for (int j = 0; j < 31; j++) {
				h = (h >>> 7) ^ h;
				h = (h << 11) ^ h;
				h = (h >>> 10) ^ h;
			}
			ht[i] = h;
		}
		return ht;
	}

	public static long compute(byte data[]) {
		return compute(data, 2166136261L, 1099511628211L);
	}

	public static long compute(byte data[], int blockId) {		
		long primes[] = {2166136261L, 1099511628211L, 100000007, 100000037, 
				100000039, 100000049, 100000073, 100000081, 100000123, 
				100000127, 100000193, 100000213, 100000217, 100000223, 
				100000231, 100000237, 100000259, 100000267, 100000279, 
				100000357, 100000379, 100000393, 100000399, 100000421, 
				100000429, 100000463, 100000469, 100000471, 100000493,
				100000541, 100000543, 100000561, 100000567, 100000577, 
				100000609, 100000627, 100000643, 100000651, 100000661, 
				100000669, 100000673, 100000687, 100000717, 100000721, 
				100000793, 100000799, 100000801, 100000837, 100000841, 
				100000853, 100000891, 100000921, 100000937, 100000939, 
				100000963, 100000969, 100001029, 100001053, 100001059,
				100001081, 100001087, 100001107, 100001119, 100001131, 
				100001147, 100001159, 100001177, 100001183, 100001203, 
				100001207, 100001219, 100001227, 100001303, 100001329, 
				100001333, 100001347, 100001357, 100001399, 100001431, 
				100001449, 100001467, 100001507, 100001533, 100001537, 
				100001569, 100001581, 100001591, 100001611, 100001623,
				100001651, 100001653, 100001687, 100001689, 100001719, 
				100001761, 100001767, 100001777, 100001791, 100001801, 
				100001809, 100001813, 100001819};

		if ((blockId % 2) == 0) {		
			return compute(data, primes[blockId], primes[blockId + 1]);
		}
		return compute2(data, primes[blockId], primes[blockId + 1]);
	}

	public static long compute(byte data[], long n, long prime) {
		for(int i = 0; i < data.length; i++) {
			n = n ^ data[i];
			n = n * prime;
		}
		return Math.abs(n);
	}

	public static long compute2(byte data[], long h, long hmult) {		

		for(int i = 0; i < data.length; i++) {
			h = (h * hmult) ^ ht[data[i] & 0xff];
			h = (h * hmult) ^ ht[(data[i] >>> 8) & 0xff];
		}
		return Math.abs(h);
	}
	
	public static byte getHashId(byte a[], byte b[], int size) {		
		for(byte i = 1; i < 100; i++) {
			long hashA = Hash.compute(a, i) % size;
			long hashB = Hash.compute(b, i) % size;
			if (hashA != hashB) {
				return i;
			}
		}		
		return -1;
	}
}
