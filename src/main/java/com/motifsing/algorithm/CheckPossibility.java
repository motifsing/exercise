package com.motifsing.algorithm;

/**
 * @author Motifsing
 */
public class CheckPossibility {

    public static void main(String[] args) {
        int[] nums1 = {4, 2, 3};
        int[] nums2 = {3, 4, 2, 3};
        int[] nums3 = {1, 2, 5, 3, 3};
        int[] nums4 = {1, 4, 1, 2};

        CheckPossibility checkPossibility = new CheckPossibility();
        System.out.println(checkPossibility.checkPossibility(nums1));
        System.out.println(checkPossibility.checkPossibility(nums2));
        System.out.println(checkPossibility.checkPossibility(nums3));
        System.out.println(checkPossibility.checkPossibility(nums4));
    }

    public boolean checkPossibility(int[] nums){
        int cnt = 0;
        for (int i = 0; i < nums.length - 1; i++) {
            if (nums[i] > nums[i+1]){
                cnt += 1;
                if (i > 0) {
                    if (nums[i+1] < nums[i-1]) {
                        nums[i+1] = nums[i];
                    }
                }
            }
        }
        return cnt <= 1;
    }
}
