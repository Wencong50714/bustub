//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager_test.cpp
//
// Identification: test/storage/disk/disk_scheduler_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <future>  // NOLINT
#include <memory>

#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(MyTest0, TestFutureGet) {
  /** The inner implementation of the scheduler is queue, so need to test there a future.get()
   *  can cause the request before this finished
   */
  char data[BUSTUB_PAGE_SIZE] = {0};
  char buf0[BUSTUB_PAGE_SIZE] = {0};
  char buf1[BUSTUB_PAGE_SIZE] = {0};
  char buf2[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto promise2 = disk_scheduler->CreatePromise();
  auto promise3 = disk_scheduler->CreatePromise();
  auto promise4 = disk_scheduler->CreatePromise();
  auto future4 = promise4.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, reinterpret_cast<char *>(&data), /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf0), /*page_id=*/0, std::move(promise2)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf1), /*page_id=*/0, std::move(promise3)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf2), /*page_id=*/0, std::move(promise4)});

  ASSERT_TRUE(future4.get());
  ASSERT_EQ(std::memcmp(buf0, data, sizeof(buf2)), 0);
  ASSERT_EQ(std::memcmp(buf1, data, sizeof(buf2)), 0);
  ASSERT_EQ(std::memcmp(buf2, data, sizeof(buf2)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

TEST(MyTest1, BinaryDataTest) {
  char buf[BUSTUB_PAGE_SIZE] = {0};
  char data[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));
  data[4] = '\0';
  data[7] = '\0';

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, reinterpret_cast<char *>(&data), /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf), /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

// NOLINTNEXTLINE
TEST(DiskSchedulerTest, ScheduleWriteReadPageTest) {
  char buf[BUSTUB_PAGE_SIZE] = {0};
  char data[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, data, /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, buf, /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

}  // namespace bustub
