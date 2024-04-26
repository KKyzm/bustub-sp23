//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  {
    auto *page1 = bpm->NewPage(&page_id_temp);

    auto guarded_page = BasicPageGuard(bpm.get(), page1);
    EXPECT_EQ(page1->GetData(), guarded_page.GetData());
    EXPECT_EQ(page1->GetPageId(), guarded_page.PageId());
    EXPECT_EQ(1, page1->GetPinCount());

    auto moved_guarded_page = BasicPageGuard(std::move(guarded_page));
    EXPECT_EQ(1, page1->GetPinCount());
    guarded_page.Drop();
    EXPECT_EQ(1, page1->GetPinCount());
    moved_guarded_page.Drop();
    EXPECT_EQ(0, page1->GetPinCount());

    bpm->FetchPage(page_id_temp);
    auto fetched_page_1 = BasicPageGuard(bpm.get(), page1);
    bpm->FetchPage(page_id_temp);
    auto fetched_page_2 = BasicPageGuard(bpm.get(), page1);
    EXPECT_EQ(2, page1->GetPinCount());

    fetched_page_2 = std::move(fetched_page_1);
    EXPECT_EQ(1, page1->GetPinCount());
    fetched_page_2.Drop();
    EXPECT_EQ(0, page1->GetPinCount());
  }

  {
    auto *page1 = bpm->NewPage(&page_id_temp);

    auto guarded_page = ReadPageGuard(bpm.get(), page1);
    EXPECT_EQ(page1->GetData(), guarded_page.GetData());
    EXPECT_EQ(page1->GetPageId(), guarded_page.PageId());
    EXPECT_EQ(1, page1->GetPinCount());

    auto moved_guarded_page = ReadPageGuard(std::move(guarded_page));
    EXPECT_EQ(1, page1->GetPinCount());
    moved_guarded_page.Drop();
    EXPECT_EQ(0, page1->GetPinCount());

    bpm->FetchPage(page_id_temp);
    auto fetched_page_1 = ReadPageGuard(bpm.get(), page1);
    bpm->FetchPage(page_id_temp);
    auto fetched_page_2 = ReadPageGuard(bpm.get(), page1);
    EXPECT_EQ(2, page1->GetPinCount());

    fetched_page_2 = std::move(fetched_page_1);
    EXPECT_EQ(1, page1->GetPinCount());
    fetched_page_2.Drop();
    EXPECT_EQ(0, page1->GetPinCount());
  }

  {
    auto *page1 = bpm->NewPage(&page_id_temp);

    auto guarded_page = WritePageGuard(bpm.get(), page1);
    EXPECT_EQ(page1->GetData(), guarded_page.GetData());
    EXPECT_EQ(page1->GetPageId(), guarded_page.PageId());
    EXPECT_EQ(1, page1->GetPinCount());

    auto moved_guarded_page = WritePageGuard(std::move(guarded_page));
    EXPECT_EQ(1, page1->GetPinCount());
    moved_guarded_page.Drop();
    EXPECT_EQ(0, page1->GetPinCount());

    bpm->FetchPage(page_id_temp);
    auto fetched_page_1 = WritePageGuard(bpm.get(), page1);
    bpm->FetchPage(page_id_temp);
    auto fetched_page_2 = WritePageGuard(bpm.get(), page1);
    EXPECT_EQ(2, page1->GetPinCount());

    fetched_page_2 = std::move(fetched_page_1);
    EXPECT_EQ(1, page1->GetPinCount());
    fetched_page_2.Drop();
    EXPECT_EQ(0, page1->GetPinCount());
  }

  {
    auto *page1 = bpm->NewPage(&page_id_temp);

    auto guarded_page = ReadPageGuard(bpm.get(), page1);
    EXPECT_EQ(page1->GetData(), guarded_page.GetData());
    EXPECT_EQ(page1->GetPageId(), guarded_page.PageId());
    EXPECT_EQ(1, page1->GetPinCount());
    {
      auto moved_guarded_page = ReadPageGuard(std::move(guarded_page));
      EXPECT_EQ(1, page1->GetPinCount());
    }
    EXPECT_EQ(0, page1->GetPinCount());
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, BPMTest_0) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto guarded_page = bpm->NewPageGuarded(&page_id_temp);
  auto* page0copy = bpm->FetchPage(page_id_temp);
  bpm->UnpinPage(page0copy->GetPageId(), false);
  EXPECT_EQ(1, page0copy->GetPinCount());
  
  auto fetched_guarded_page0_0 = bpm->FetchPageRead(page_id_temp);
  EXPECT_EQ(2, page0copy->GetPinCount());

  auto fetched_guarded_page0_1 = bpm->FetchPageRead(page_id_temp);
  EXPECT_EQ(3, page0copy->GetPinCount());
  
  auto moved_0 = std::move(fetched_guarded_page0_0);
  EXPECT_EQ(3, page0copy->GetPinCount());

  auto moved_1 = ReadPageGuard(std::move(fetched_guarded_page0_1));
  EXPECT_EQ(3, page0copy->GetPinCount());
  
  fetched_guarded_page0_0.Drop();
  EXPECT_EQ(3, page0copy->GetPinCount());
  fetched_guarded_page0_0.Drop();
  EXPECT_EQ(3, page0copy->GetPinCount());
  auto unuseful = ReadPageGuard(std::move(fetched_guarded_page0_1));
  EXPECT_EQ(3, page0copy->GetPinCount());
  fetched_guarded_page0_1.Drop();
  EXPECT_EQ(3, page0copy->GetPinCount());
  unuseful.Drop();
  EXPECT_EQ(3, page0copy->GetPinCount());
  
  moved_0.Drop();
  EXPECT_EQ(2, page0copy->GetPinCount());
  moved_1.Drop();
  EXPECT_EQ(1, page0copy->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, BPMTest_1) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto guarded_page = bpm->NewPageGuarded(&page_id_temp);
  auto* page0copy = bpm->FetchPage(page_id_temp);
  bpm->UnpinPage(page_id_temp, false);
  EXPECT_EQ(1, page0copy->GetPinCount());
  
  auto fetched0 = bpm->FetchPageRead(page_id_temp);
  EXPECT_EQ(2, page0copy->GetPinCount());
  
  auto fetched1 = bpm->FetchPageRead(page_id_temp);
  EXPECT_EQ(3, page0copy->GetPinCount());
  
  // a deadlock is expected to occur
  // auto fetched2 = bpm->FetchPageWrite(page_id_temp);

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}
}  // namespace bustub
