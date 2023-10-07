/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {
TEST(MyTest0, K_is_1) {
  // when k is 1, each access cause a rearrange
  LRUKReplacer lru_replacer(7, 1);

  // GENERAL TEST
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());      // [] [1,2,3,4,5,{6}]

  // TEST FOR THE ORDER OF LIST
  lru_replacer.RecordAccess(1);   // [] [2,3,4,5,{6},1]

  int value;
  lru_replacer.Evict(&value);     // [] [3,4,5,{6},1]
  ASSERT_EQ(2, value);
  ASSERT_EQ(4, lru_replacer.Size());

  lru_replacer.Evict(&value);     // [] [4,5,{6},1]
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  lru_replacer.Evict(&value);     // [] [5,{6},1]
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  lru_replacer.Evict(&value);     // [] [{6},1]
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(2, lru_replacer.Size());

  lru_replacer.Evict(&value);     // [] [1]
  ASSERT_EQ(6, value);
  ASSERT_EQ(1, lru_replacer.Size());

  lru_replacer.Evict(&value);     // empty
  ASSERT_EQ(1, value);
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(MyTest1, test_remove) {
  LRUKReplacer lru_replacer(7, 2);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);

  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);

  ASSERT_EQ(2, lru_replacer.Size());
  lru_replacer.Remove(1);
  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Remove(2);
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(MYTest2, test_less_k_list) {
  // less list is arrange by original timestamp, test this case
  LRUKReplacer lru_replacer(7, 5);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);

  // As long as access time less than k, access to item don't change list layout
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(4);

  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(1, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(5, value);
}

TEST(MyTest3, test_more_k_list) {
  LRUKReplacer lru_replacer(7, 2);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);

  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);

  // the order depend on the newest access
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);

  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(5, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(1, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
}

TEST(LRUKReplacerTest, SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // This operation should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
}
}  // namespace bustub
