package com.sample.filters.bloom;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterDriver {

	public static void main(String[] args) throws IOException {
		
		Path in = new Path(args[0]);
		int numMember = Integer.parseInt(args[1]);
		float flasePositve = Float.parseFloat(args[2]);
		Path bfFile  = new Path(args[3]);
		
		int vectorsize = getOptimalBloomFilterSize(numMember, flasePositve);
		int nbHash = getOptimalK(numMember ,vectorsize);
		
		BloomFilter bloomFilter = new BloomFilter(vectorsize, nbHash, Hash.MURMUR_HASH);
		
		FileSystem fileSystem = FileSystem.get(new Configuration());
		
		FileStatus[] listStatus = fileSystem.listStatus(in);
		int numElements = 0;
		for( FileStatus status :   listStatus ){
			
			String line = null;
			
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(fileSystem.open(status.getPath()))));
			
			while( (line = bufferedReader.readLine()) != null){
				bloomFilter.add(new Key(line.getBytes()));
				++numElements;
			}
			
			bufferedReader.close();
			
		}
		
		
		System.out.println("Training bloom filter of size " + vectorsize + " with " + nbHash + " hash functions, "
				+ numMember + " approximate number of records, and " + flasePositve + " false positive rate");
		
		
		
		// serialize bloom filter to HDFS
		FSDataOutputStream dataInputStream = fileSystem.create(bfFile);
		bloomFilter.write(dataInputStream);
		dataInputStream.flush();
		dataInputStream.close();
		
		System.exit(0);

	}
	
	/**
	 * 
	 * @param numMember number of elements used to train set
	 * @param vectorsize size of bloom filter
	 * 
	 * k = m*log(2) / n 
	 * 
	 * where k = optimal k 
	 * m = bloom filter optimal size 
	 * n = number of elements
	 * 
	 */
	private static int getOptimalK(int numMember, int vectorsize) {
		return (int)Math.round(vectorsize * Math.log(2) / numMember);
	}

	/**
	 * 
	 * @param numElement Number of elements used to train set 
	 * @param falsePosRate desired false positive rate 
	 * @return optimal bloom filter size
	 * 
	 * m = -n*log(p)/(log2)^2 
	 * 
	 * where n = number of element 
	 * p = false positive rate 
	 *  
	 */
	public static int getOptimalBloomFilterSize(int numElement, float falsePosRate ){
			
		
		return (int)(-numElement * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2)); 
		
	}

}
