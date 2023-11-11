#include "BoundedBuffer.h"
#include <iostream>
using namespace std;


BoundedBuffer::BoundedBuffer (int _cap) : cap(_cap) {
    // modify as needed
}

BoundedBuffer::~BoundedBuffer () {
    // modify as needed
}


void BoundedBuffer::push (char* msg, int size) {
    // 1. Convert the incoming byte sequence given by msg and size into a vector<char>
    //std::lock_guard<std::mutex> lock(m);
    std::unique_lock<std::mutex> lock(m);
    //std::cout<<"THREAD LOCKED IN PUSH"<<std::endl;
    notFull.wait(lock, [this] { return (int)q.size() <= cap; });
    //std::cout<<"THREAD DONE WAITING IN PUSH"<<std::endl;
    
    std::vector<char> msgBuff;
    for(int i = 0; i < size; i++){
        msgBuff.push_back(msg[i]);
    }
    
    // 2. Wait until there is room in the queue (i.e., queue lengh is less than cap)
    // while(q.size() >= (long unsigned int)cap)
    // 3. Then push the vector at the end of the queue
    q.push(msgBuff);
    // 4. Wake up threads that were waiting for push
    notEmpty.notify_one();
    
}

int BoundedBuffer::pop (char* msg, int size) {
    // 1. Wait until the queue has at least 1 item
    std::unique_lock<std::mutex> lock(m);
    //while(q.empty()){
    //    c.wait(lock);
    //}
    //std::cout<<"THREAD LOCKED IN POP"<<std::endl;

    notEmpty.wait(lock, [this] { return !q.empty(); });
    
    //std::cout<<"THREAD DONE WAITING IN POP"<<std::endl;
    // 3. Convert the popped vector<char> into a char*, copy that into msg; assert that the vector<char>'s length is <= size
    int length = q.front().size();
    assert(length <= size);
    memcpy(msg, q.front().data(), length);
    q.pop();

    notFull.notify_one();
    // 4. Wake up threads that were waiting for pop
    // 5. Return the vector's length to the caller so that they know how many bytes were popped
    return length;
}

size_t BoundedBuffer::size () {
    return q.size();
}
