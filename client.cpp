#include <fstream>
#include <iostream>
#include <thread>
#include <sys/time.h>
#include <sys/wait.h>

#include "BoundedBuffer.h"
#include "common.h"
#include "Histogram.h"
#include "HistogramCollection.h"
#include "FIFORequestChannel.h"
#include <tuple>
// ecgno to use for datamsgs
#define EGCNO 1
std::mutex fm;

using namespace std;

void patient_thread_function(BoundedBuffer *buffer, int n, int patient) {
    for(int r = 0; r < n; r++){
        for(int i = 0; i < 1; i++){
            datamsg *msgObj1 = new datamsg(patient, r*0.004, EGCNO);
            buffer->push((char*)msgObj1, sizeof(datamsg));
            delete msgObj1;
        }
    }
}

void file_thread_function (BoundedBuffer *reqBuffer, std::string filename, FIFORequestChannel *chan, int maxMessage) {
   /*
    * Pushes all the file chunks to the request buffer
    *
    * Try request the max amount of bytes per message
    *
    * */ 
    filemsg fm(0, 0);
    
    int len = sizeof(filemsg) + (filename.size() + 1);
    char* buf2 = new char[len]; // buffer for the file length requestv
    memcpy(buf2, &fm, sizeof(filemsg));
    strcpy(buf2 + sizeof(filemsg), filename.c_str());
    chan->cwrite(buf2, len);  // I want the file length;
    
    __int64_t bytes; // response buffer for file length
    chan->cread(&bytes, sizeof(__int64_t)); // Give me the file length
    
    __int64_t remainder = bytes % maxMessage;
    __int64_t offset = 0;	
    for(int i = 1; i <= bytes/maxMessage; i++){
        filemsg filereq(offset, maxMessage);
        std::pair<int, filemsg> reqPayload = std::make_pair(maxMessage, filereq);
        reqBuffer->push((char*)&reqPayload, sizeof(std::pair<int, filemsg>));
        offset += maxMessage;
    }
    if(remainder){
        filemsg filereq(offset, remainder);
        std::pair<int, filemsg> reqPayload = std::make_pair(remainder, filereq);                
        reqBuffer->push((char*)&reqPayload, sizeof(std::pair<int, filemsg>));
 
    }
    delete[] buf2;

}


void transfer_bytes(int fd, int offset, char *buf, int bytes){
    fm.lock();
    lseek(fd, offset, SEEK_SET);
    write(fd, buf, bytes);
    fm.unlock();
}

void worker_thread_function(BoundedBuffer *respBuffer, BoundedBuffer *reqBuffer, FIFORequestChannel *chan, const int maxMessage, std::string filename, int transferFd) {
    // pop the request message from the req buffer
    while(true){
        char *reqMsg = new char[maxMessage];
        
        int bytes = reqBuffer->pop(reqMsg, maxMessage);
        
        // Case 1: (DATA_MSG)
        if(bytes == sizeof(datamsg)){
            datamsg *d = (datamsg *)reqMsg;
            if(d->mtype == QUIT_MSG){
                delete[] reqMsg;
                break;
            }
            chan->cwrite(reqMsg, sizeof(datamsg));
            // read the response from server
            double resp;
            chan->cread(&resp, sizeof(double));
            // push response data to the response buffer
            std::tuple<int, double, MESSAGE_TYPE> respObj(d->person, resp, d->mtype);
            char *respBytes = (char *)&respObj;
            respBuffer->push(respBytes, sizeof(std::tuple<int, double, MESSAGE_TYPE>));
        }
        // Case 2:(FILE_MSG)
        else{
            std::pair<int, filemsg> * f = (std::pair<int, filemsg>*)reqMsg;
            if(f->second.mtype == QUIT_MSG){
                delete[] reqMsg;
                break;
            }
            // prep file request message
            const int reqSize = sizeof(filemsg) + filename.size() + 1;
            char *fileReqMsg = new char[reqSize];
            memcpy(fileReqMsg, &f->second, sizeof(filemsg));
            strcpy(fileReqMsg + sizeof(filemsg), filename.c_str());
            // send file request message
            chan->cwrite(fileReqMsg, sizeof(filemsg) + filename.size() + 1);
            // read the byte chunk from the file
            char *fileRespBuf = new char[maxMessage];
            chan->cread(fileRespBuf, f->second.length);
            // write bytes to the open transfer file at the correct offset
            transfer_bytes(transferFd, f->second.offset, fileRespBuf, f->second.length); 
            delete[] fileRespBuf;
            delete[] fileReqMsg; 
        }
        
        delete[] reqMsg;
    }

}

void histogram_thread_function (BoundedBuffer *respBuffer, HistogramCollection *collection, const int maxMessage) {
    // functionality of the histogram threads
    // pop response from the response buffer
    while(true){
        char respMsg[sizeof(std::tuple<int, double, MESSAGE_TYPE>)];
       
        respBuffer->pop(respMsg, maxMessage);
        
        std::tuple<int, double, MESSAGE_TYPE> *respData = (std::tuple<int, double, MESSAGE_TYPE>*) respMsg;
        // check if we have a quit message
        if(std::get<MESSAGE_TYPE>(*respData) == QUIT_MSG){
            break;
        }
        
        // update the collection
        collection->update(std::get<int>(*respData), std::get<double>(*respData));
    }

}


int main (int argc, char* argv[]) {
    int n = 1000;	// default number of requests per "patient"
    int p = 10;		// number of patients [1,15]
    int w = 100;	// default number of worker threads
	int h = 20;		// default number of histogram threads
    int b = 20;		// default capacity of the request buffer (should be changed)
	int m = MAX_MESSAGE;	// default capacity of the message buffer
	string f = "";	// name of file to be transferred
    
    // read arguments
    int opt;
	while ((opt = getopt(argc, argv, "n:p:w:h:b:m:f:")) != -1) {
		switch (opt) {
			case 'n':
				n = atoi(optarg);
                break;
			case 'p':
				p = atoi(optarg);
                break;
			case 'w':
				w = atoi(optarg);
                break;
			case 'h':
				h = atoi(optarg);
				break;
			case 'b':
				b = atoi(optarg);
                break;
			case 'm':
				m = atoi(optarg);
                break;
			case 'f':
				f = optarg;
                break;
		}
	}
    
	// fork and exec the server
    int pid = fork();
    if (pid == 0) {
        execl("./server", "./server", "-m", (char*) to_string(m).c_str(), nullptr);
    }
    
	// initialize overhead (including the control channel)
	FIFORequestChannel* control = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);
    BoundedBuffer request_buffer(b);
    BoundedBuffer response_buffer(b);
	HistogramCollection hc;

    // making histograms and adding to collection
    for (int i = 0; i < p; i++) {
        Histogram* h = new Histogram(10, -2.0, 2.0);
        hc.add(h);
    }
	
	// record start time
    struct timeval start, end;
    gettimeofday(&start, 0);

    /* create all threads here */
    std::vector<thread> fthreads;
    std::vector<thread> pthreads;
    std::vector<thread> wthreads;
    std::vector<thread> hthreads;
    int fd;
    // start file transfer thread if we have a file
    if(f != ""){
        std::string filepath = "./received/" + f;
        fd = open(filepath.c_str(), O_CREAT | O_WRONLY, S_IRWXU); 
        fthreads.emplace_back(file_thread_function, &request_buffer, f, control, m); 
    }
   
    for(int i = 0;i < p; i++){
        pthreads.emplace_back(patient_thread_function, &request_buffer, n, i+1);
    }

    std::vector<FIFORequestChannel*> channels;
    channels.push_back(control);
    char newChannelName[64];
    for(int i = 0; i < w; i++){
        MESSAGE_TYPE mType = NEWCHANNEL_MSG;
        control->cwrite(&mType, sizeof(MESSAGE_TYPE));
        control->cread(&newChannelName, 64);
        FIFORequestChannel * chan = new FIFORequestChannel(std::string(newChannelName), FIFORequestChannel::CLIENT_SIDE);
        channels.push_back(chan);
        wthreads.emplace_back(worker_thread_function, &response_buffer, &request_buffer, chan, m, f, fd);
    }

    for(int i = 0; i < h; i++){
        hthreads.emplace_back(histogram_thread_function, &response_buffer, &hc, m);            
    }

    for(auto& thread: fthreads){
        thread.join();
    }
    // Join threads from the first vector
    for (auto& thread : pthreads) {
        thread.join();
    }


    // send quit messages to all w workers
    for(int i = 0; i < w; i++){
        datamsg d(0,0,0);
        d.mtype = QUIT_MSG;
        request_buffer.push((char*)&d, sizeof(datamsg));
    }

    // Join threads from the second vector
    for (auto& thread : wthreads) {
        thread.join();
    }

    for(int i = 0; i < h; i++){
        datamsg d(0,0,0);
        d.mtype = QUIT_MSG;
        response_buffer.push((char*)&d, sizeof(datamsg));
    }

    // Join threads from the third vector
    for (auto& thread : hthreads) {
        thread.join();
    }
    
    
	// record end time
    gettimeofday(&end, 0);

    // print the results
	if (f == "") {
		hc.print();
	}
    int secs = ((1e6*end.tv_sec - 1e6*start.tv_sec) + (end.tv_usec - start.tv_usec)) / ((int) 1e6);
    int usecs = (int) ((1e6*end.tv_sec - 1e6*start.tv_sec) + (end.tv_usec - start.tv_usec)) % ((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;
    
    
    if(fd){
        close(fd);
    }
	// quit and close control channel
    
    MESSAGE_TYPE q = QUIT_MSG;
    for(long unsigned int i = 0; i < channels.size(); i++){
        channels[i]->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
        delete channels[i];
    }
    cout << "All Done!" << endl;

	// wait for server to exit
	wait(nullptr);
}
