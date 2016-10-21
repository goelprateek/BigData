package com.sample.training.sort;

public class QuickSort {

	private static <T extends Comparable<T>> int split(T[] list, int low, int hi) {

		T pivot = list[0];
		int left = low + 1;
		int right = hi;

		while (true) {
			while (left <= right) {

				if (list[left].compareTo(pivot) < 0) {
					left++;
				} else {
					break;
				}

			}
			while (right > left) {
				if (list[right].compareTo(pivot) < 0) {
					break;
				} else {
					right--;
				}
			}

			if (left >= right) { // break the loop
				break;
			}

			// swap left right items
			T temp = list[left];
			list[left] = list[right];
			list[right] = temp;
			// advances there respective pointers
			left++;
			right--;

			

		}
		
		// swap pivot with left -1 position
		list[low] = list[left-1];
		list[left-1] = pivot;

		return left - 1;
	}

	private static <T extends Comparable<T>> void sort(T[] list, int low, int hi) {

		if ((hi - low) <= 0) { // fewer then 2 items
			return;
		}

		int splitPoint = split(list, low, hi); 
		split(list, 0, splitPoint - 1);
		split(list, splitPoint + 1, hi);
	}

	public static <T extends Comparable<T>> void sort(T[] list) {
		
		if (list.length <= 1) {
			return;
		}

		sort(list, 0, list.length - 1);
	}

}
