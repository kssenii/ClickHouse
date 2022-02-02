#pragma once
#include <iomanip>
#include <iostream>
#include <gtest/gtest.h>
#include <Common/FileCache.h>
#include <Common/filesystemHelpers.h>
#include <Common/SipHash.h>
#include <Common/hex.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;

[[maybe_unused]] static void assertRange(
    [[maybe_unused]] size_t assert_n, DB::FileSegmentPtr file_segment,
    const DB::FileSegment::Range & expected_range, DB::FileSegment::State expected_state)
{
    auto range = file_segment->range();

    std::cerr << fmt::format("\nAssert #{} : {} == {} (state: {} == {})\n", assert_n,
                             range.toString(), expected_range.toString(),
                             toString(file_segment->state()), toString(expected_state));

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_segment->state(), expected_state);
};

void printRanges(const auto & segments)
{
    std::cerr << "\nHaving file segments: ";
    for (const auto & segment : segments)
        std::cerr << '\n' << segment->range().toString() << " (state: " + DB::FileSegment::stateToString(segment->state()) + ")" << "\n";
}

[[maybe_unused]] static std::vector<DB::FileSegmentPtr> fromHolder(const DB::FileSegmentsHolder & holder)
{
    return std::vector<DB::FileSegmentPtr>(holder.file_segments.begin(), holder.file_segments.end());
}

[[maybe_unused]] static String keyToStr(const DB::FileCache::Key & key)
{
    return getHexUIntLowercase(key);
}

[[maybe_unused]] static String getFileSegmentPath(const String & base_path, const DB::FileCache::Key & key, size_t offset)
{
    auto key_str = keyToStr(key);
    return fs::path(base_path) / key_str.substr(0, 3) / key_str / DB::toString(offset);
}
