package com.sample.training;

import com.sample.training.sort.QuickSort;

public class QuickSortExample {

	public static void main(String[] args) {
		
		Integer[] list = {5,2,8,2}; 
		
		System.out.println("Before");
		print(list);
		QuickSort.sort(list);
		System.out.println("After");
		print(list);

	}
	
	
	 private static <T extends Comparable<T>>  void print(T[] list){
		for (T t : list) {
			System.out.print(t.toString());
		}
	}

}
