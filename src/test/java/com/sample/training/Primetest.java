package com.sample.training;

import java.text.ParseException;

public class Primetest {

	private static boolean isSqrt(int n){
		if(Math.sqrt(n)%1==0){
			return true;
		}else{
			return false;
		}
	}
	
	private static int totalSquareInRangeCount(int lower , int upper){
		
		if(lower == upper){
			if(isSqrt(lower)){
				return 1;
			}
		}else{
			if(isSqrt(lower+1)){
				return  (int)((Math.round(Math.sqrt(upper)) - Math.round(Math.sqrt(lower)))+1) ;	
			}else{
				return (int)((Math.round(Math.sqrt(upper)) - Math.round(Math.sqrt(lower))) -1 ) ;
			}
		}
		
		
		return 0;
		
	}
	
	public static void main(String[] args) throws ParseException {
		 
		
		System.out.println(totalSquareInRangeCount(11, 74));
		
		/*long count = (Math.round(Math.sqrt(11)) - Math.round(Math.sqrt(2)));
		System.out.println(count);
		
		//System.out.println(((1<<((5>>1)+1))-1) << 5%2);
		for (int i = 2; i <= 1; i++) {
			if (isPrime(i)) {
				System.out.println(i);
			}
		}*/

	}

	public static boolean isPrime(int num) {

		if (num == 2 || num == 3) {
			return true;
		}

		if (num % 2 == 0 || num % 3 == 0) {
			return false;
		}

		for (int i = 3; i < Math.sqrt(num); i++) {

			if (num % i == 0 || num % Math.sqrt(num) == 0) {
				return false;
			}
		}

		return true;
	}

	public static boolean checkPallindrome(int num) {
		int number = num;
		int reverse = 0;

		while (num != 0) {
			int reminder = num % 10;
			reverse = reverse * 10 + reminder;
			num = num / 10;
		}

		if (number == reverse) {
			return true;
		}

		return false;
	}

}
