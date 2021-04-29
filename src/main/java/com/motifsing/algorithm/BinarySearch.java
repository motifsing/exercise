package com.motifsing.algorithm;

/**
 * @ClassName BinarySearch
 * @Description 二分查找
 * @Author Motifsing
 * @Date 2021/4/24 9:17
 * @Version 1.0
 **/
public class BinarySearch {
    public static void main(String[] args) {
        int[] nums = new int[]{-1, 0, 3, 5, 9, 12};

        int target1 = 2;
        System.out.println(search(nums, target1));

        int target2 = -1;
        System.out.println(search(nums, target2));

        int target3 = 12;
        System.out.println(search(nums, target3));

        int target4 = 3;
        System.out.println(search(nums, target4));

        int target5 = 5;
        System.out.println(search(nums, target5));

        int target6 = -2;
        System.out.println(search(nums, target6));

        int target7 = 14;
        System.out.println(search(nums, target7));

        int target8 = 5;
        int[] nums2 = new int[]{1};
        System.out.println(search(nums2, target8));

        int target9 = 1;
        System.out.println(search(nums2, target9));

        int[] nums3 = new int[]{2, 5};
        int target10 = 0;
        System.out.println(search(nums3, target10));
    }

    public static int search(int[] nums, int target) {
        int right = nums.length - 1;
        int left = 0;
        while (true){
            if (right - left <= 1) {
                if (nums[left] == target) {
                    return left;
                } else if (nums[right] == target) {
                    return right;
                } else {
                    return -1;
                }
            }

            int mid = left + (right - left) / 2;
            if (nums[mid] == target){
                return mid;
            } else if (nums[mid] < target) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }

        }
    }
}
