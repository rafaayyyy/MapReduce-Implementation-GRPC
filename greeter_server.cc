#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <fstream>
#include <hdfs.h>
#include <sstream>

using namespace std::chrono;
using namespace std;

#include <grpcpp/ext/proto_server_reflection_plugin.h> //automatically hardcoded for the file generation from makefile
#include <grpcpp/grpcpp.h> 
#include <grpcpp/health_check_service_interface.h>

#ifdef BAZEL_BUILD                                  //automatically hardcoded for the file generation from makefile
#include "examples/protos/helloworld.grpc.pb.h"         
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Server;                     //Grpc methods
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using helloworld::Greeter;                  //from helloworld proto file
using helloworld::HelloReply;               //from helloworld proto file
using helloworld::HelloRequest;             //from helloworld proto file
using helloworld::MapRequest;
using helloworld::MapReply;
using helloworld::ReduceRequest;
using helloworld::ReduceReply;



string mapInput = "";
string reduceInput = "";
unordered_map<string, int> counts;
unordered_map<string, int> word_counts;
string percentDone = "0";
// void sleepFunc(){
//   cout << mapInput << endl;
//   usleep(5000000);
//   percentDone = "100";
// }

void top10Occurences(){
  cout << "Reduce Phase Done" << endl << endl;
  int N;
  cout << "Enter the value of N: ";
  cin >> N;

  vector<pair<string, int>> sorted_words(word_counts.begin(), word_counts.end());
  sort(sorted_words.begin(), sorted_words.end(), [](const auto& x, const auto& y)
  { return x.second > y.second; });

  cout << "Top " << N << " words:" << endl;
  for (int i = 0; i < N && i < sorted_words.size(); i++) {
      cout << sorted_words[i].first << " : " << sorted_words[i].second << endl;
  }
}

void MapperFunction() {
  // cout << mapInput << endl;
  // usleep(5000000);
  char taskNum = mapInput.at(0);
  mapInput = mapInput.substr(1);
  // cout << mapInput << endl;
  istringstream input_stream(mapInput);
  string line;
  while (getline(input_stream, line)) {
    istringstream line_stream(line);
    string word;
    while (line_stream >> word) {
        counts[word]++;
    }
  }
  string filename = "MapOutputTask" + string(1, taskNum) + ".txt";
  ofstream output_file(filename);
  for (auto pair = counts.begin(); pair != counts.end(); ++pair) {
      output_file << pair->first << " " << pair->second << endl;
  }
  output_file.close();
  usleep(3000000);
  percentDone = "100";
}

void ReducerFunction() {
  ifstream input_files[4];
  input_files[0].open("MapOutputTask0.txt");
  input_files[1].open("MapOutputTask1.txt");
  input_files[2].open("MapOutputTask2.txt");
  input_files[3].open("MapOutputTask3.txt");

  string line;
  for (int i = 0; i < 4; i++) {
    while (getline(input_files[i], line)) {
      reduceInput += line + '\n';
    }
    input_files[i].close();
  }
  string word;
  int count;
  istringstream iss(reduceInput);
  while (iss >> word >> count) {
    word_counts[word] += count;
  }
  // for (const auto& wc : word_counts) {
  //   cout << wc.first << " " << wc.second << endl;
  // }

  string output_path = "/Assignment5/ReducedOutput.txt";
  string hdfs_command = "hdfs dfs -put -f - " + output_path;
  FILE* output_file = popen(hdfs_command.c_str(), "w");

  for (const auto& wc : word_counts) {
    fprintf(output_file, "%s %d\n", wc.first.c_str(), wc.second);
  }
  top10Occurences();
}



class GreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {         //to deal with the request and response accordingly
    reply->set_message(percentDone);                //the reply is set as request data and sent back meaning the slave is online
    if(reply->message() == "100"){
      percentDone = "0";
    }
    return Status::OK;
  }

  Status Mapper(ServerContext* context, const MapRequest* request,
                MapReply* reply) override {
    string ran = "0";
    mapInput = request->name();
    thread T1 = thread(MapperFunction);
    T1.detach();
    ran = percentDone;
    reply->set_message(ran);
    return Status::OK;
  }

  Status Reducer(ServerContext* context, const ReduceRequest* request,
                ReduceReply* reply) override {
    thread T2 = thread(ReducerFunction);
    T2.detach();
    string ran = "reduce done";
    reply->set_message(ran);
    return Status::OK;
  }
};

void RunServer() {
  string portNum = "";
  cout << "Enter the socket number of client (e.g 10000):" << flush;
  cin >> portNum;
  string server_address("0.0.0.0:" + portNum);    //to create a new slave on given port number
  GreeterServiceImpl service;

  ServerBuilder builder;

  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());   //Server set on listening mode for requests by the master

  builder.RegisterService(&service);        //service is registered through which we will communicate between master and clients

  unique_ptr<Server> server(builder.BuildAndStart());       //Server starts
  cout << "Slave Node Port Number : " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  setenv("CLASSPATH", "/home/rafay/hadoop-3.2.3/etc/hadoop:/home/rafay/hadoop-3.2.3/share/hadoop/common/:/home/rafay/hadoop-3.2.3/share/hadoop/common/lib/:/home/rafay/hadoop-3.2.3/share/hadoop/hdfs/:/home/rafay/hadoop-3.2.3/share/hadoop/hdfs/lib/:/home/rafay/hadoop-3.2.3/share/hadoop/mapreduce/:/home/rafay/hadoop-3.2.3/share/hadoop/mapreduce/lib/", 1);
  RunServer();
  return 0;
}