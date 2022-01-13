#pragma once
#include <IO/BufferBase.h>

namespace DB
{

class BufferSwapHelper
{
public:
    BufferSwapHelper(BufferBase & buffer1_, BufferBase & buffer2_)
        : buffer1(buffer1_), buffer2(buffer2_)
    {
        buffer1.swap(buffer2);
    }

    ~BufferSwapHelper()
    {
        buffer1.swap(buffer2);
    }

    BufferSwapHelper(const BufferSwapHelper & other) = delete;

private:
    BufferBase & buffer1;
    BufferBase & buffer2;
};

}
