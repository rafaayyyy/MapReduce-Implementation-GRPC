#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <iomanip>
#include <sys/ioctl.h>
#include <termios.h>
#include <fstream>
#include <hdfs.h>
using namespace std::chrono;
using namespace std;


#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD          //automatically hardcoded for the file generation from makefile
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Channel;             //Grpc methods
using grpc::ClientContext;
using grpc::Status;
using helloworld::Greeter;       //from helloworld proto file
using helloworld::HelloReply;    //from helloworld proto file
using helloworld::HelloRequest;  //from helloworld proto file
using helloworld::MapRequest;
using helloworld::MapReply;
using helloworld::ReduceRequest;
using helloworld::ReduceReply;

class GreeterClient {
 public:
  GreeterClient(shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  string SayHello(const std::string& user) {
      HelloRequest request;       //for string we send to slave
      request.set_name(user);     //containg hardcoded"request"

      HelloReply reply;           //to recieve data from slave

      ClientContext context;      //context for the slave or client to specify extra informations

      Status status = stub_->SayHello(&context, request, &reply);        //Status for RPC connection

      if (status.ok()) {
          return reply.message();          //Connection and reply successful
      }
      else {
          return "RPC failed";             //Connection unsuccessful
      }
  }

  string MapperReq(const std::string& user) {
      MapRequest mapReq;
      mapReq.set_name(user);

      MapReply mapReply;

      ClientContext context;

      Status status = stub_->Mapper(&context, mapReq, &mapReply);

      if (status.ok()) {
          return mapReply.message();
      }
      else {
          return "RPC failed";
      }
  }

  string ReducerReq(const std::string& user) {
      ReduceRequest redReq;
      redReq.set_name(user);

      ReduceReply redReply;

      ClientContext context;

      Status status = stub_->Reducer(&context, redReq, &redReply);

      if (status.ok()) {
          return redReply.message();
      }
      else {
          return "RPC failed";
      }
  }
  private:
    unique_ptr<Greeter::Stub> stub_;
};

bool kbhit()
{
    termios term;
    tcgetattr(0, &term);

    termios term2 = term;
    term2.c_lflag &= ~ICANON;
    tcsetattr(0, TCSANOW, &term2);

    int byteswaiting;
    ioctl(0, FIONREAD, &byteswaiting);

    tcsetattr(0, TCSANOW, &term);

    return byteswaiting > 0;
}

vector<GreeterClient> slaves;  
vector<bool> online={false,false,false,false};        //online status for every slave node
vector<bool> availableServers={true,true,true,true};
vector<bool> taskCompleted={false,false,false,false};
vector<int> serverNum ={-1,-1,-1,-1};
vector<bool> taskAssigned={false,false,false,false};
string filecontents;
vector<int> linesPerTask;
bool reduceDone = false;
vector<string> taskStrings(4);
bool checkAssignment(int n){
  for(int i =0;i<4;i++){
    if(serverNum[i] == n){
      return false;
    }
  }
  return true;
}

int tasks = 0;

void HeartBeat(){
  unsigned int ms = 1000000;            //1 second time interval for control interval
  chrono::seconds timeout(4);           //timeout interval
  char choice = 'N';
  unsigned int t;

  while(true){
    for (int i=0;i<4;i++){
          string request("request");                //to send request by the master
          auto t1 = chrono::system_clock::now();
          string response = slaves[i].SayHello(request);        //request sent to slave using SayHello Method and response noted
          auto t2 = chrono::system_clock::now();
          auto timeTaken = chrono::duration_cast<chrono::seconds>(t2 - t1);   //time noted for the response to come back
          // if(timeTaken > timeout){                        //If it took more than 4 seconds to reply, it is declared unresponsive
          //   online[i] = false; 
          // }
          // else{
            if(response == "100"){             //Declared unresponsive for empty string and responsive
              online[i] = true;                 //when same request was sent in its response (check slave code)
              availableServers[i] = true;
              tasks++;
              for(int k =0;k<4;k++){
                if(serverNum[k] == i){
                  taskCompleted[k] = true;
                  serverNum[k] = -1;
                }
              }
            }
            else if(response == "0"){
                online[i] = true;
                availableServers[i] = checkAssignment(i);
            }
            else {
                online[i] = false;
                availableServers[i] = false;
                for(int k =0;k<4;k++){
                  if(serverNum[k] == i){
                    taskCompleted[k] = false;
                    serverNum[k] = -1;
                    taskAssigned[k] = false;
                    cout << endl << endl << endl <<"Task number " << k << " failed on slave number " << i << endl;
                    cout << "THE TASK WILL NOW BE REASSIGNED!!!" << endl << endl;
                    break;
                }
              }
            }
          //  }
          }
    for(int j=0;j<4;j++)
    {
      cout << endl << "SLAVE NODE " << j << " STATUS:" << endl;
      cout << "Active :";
      if(online[j] == true){
        // cout << "Slave Node Num " << j << " is responsive" << endl;
        cout << setw(10) << "ONLINE";
      }
      else{
        // cout << "Slave Node Num " << j << " is unresponsive" << endl;
        cout << setw(10) << "OFFLINE";
      }
      cout << "       Availability :";
      if(availableServers[j] == true and online[j] == true){
        cout << setw(10) << "AVAILABLE";
      }
      else{
        cout << setw(10) << "UNAVAILABLE";
      }
    }
    for(int j=0;j<4;j++)
    {
      cout << endl<< "TASK NUMBER " << j << " STATUS:" << endl;
      cout << "Assignment :";
      if(taskAssigned[j] == true){
        cout << setw(10) << "ASSIGNED";
      }
      else{
        cout << setw(10) << "NOT ASSIGNED";
      }
      cout << "       Task Completion :";
      if(taskCompleted[j] == true){
        cout << setw(10) << "COMPLETED";
      }
      else{
        cout << setw(10) << "INCOMPLETE";
      }
      if(serverNum[j] == -1 && taskCompleted[j] == false){
        cout << endl <<"Task Not Assigned Yet";
      }
      else if (serverNum[j] == -1 && taskCompleted[j] == true){
      }
      else{
        cout << endl <<"Currently assigned to slave number: " << serverNum[j];
      }
    }
    cout << endl << endl << "===========================================================";
    cout << endl << endl;
    if(tasks == 4){
      break;
    }
    if(kbhit()){
      cin.ignore();
      cout << "Enter the duration of Control Interval in seconds : ";   
      cin >> t;         //to take unsigned integer value
      ms = t * 1000000;         //usleep uses microseconds so multiply with 1000000 to make it in seconds
      usleep(ms);
      cin.ignore();
    }
    else {
      usleep(ms);
    }
    // cout << "Do you want to change control interval? Press Y for yes:";  //To check if user wants to change time interval
    // cin >> choice;
    // if(choice == 'Y' or choice == 'y'){
    //     cin.ignore();
    //     cout << "Enter the duration of Control Interval in seconds : ";   
    //     cin >> t;         //to take unsigned integer value
    //     ms = t * 1000000;         //usleep uses microseconds so multiply with 1000000 to make it in seconds
    //     choice = 'N';
    //     usleep(ms);         //sleep period for control interval
    // }
    // else {
    //    usleep(ms);         //sleep period for control interval
    // }
  }
  cout << "Reduce Phase" << endl;
}
void taskAssignment(){
  while (tasks < 4){
    for(int i=0;i<4;i++){
      if(taskAssigned[i] == false){
        for(int j =0; j<4; j++){
          if(availableServers[j] == true && online[j] == true) {
            string request = to_string(i) + taskStrings[i];
            taskAssigned[i] = true;                 //when same request was sent in its response (check slave code)
            availableServers[j] = false;
            serverNum[i] = j;               //to send request by the master
            string response = slaves[j].MapperReq(request);        //request sent to slave using MapperReq Method and response noted
            break;
          }
        }
      }
    }
  }
  while(true){
    for(int j =0; j<4; j++){
      if(availableServers[j] == true && online[j] == true) {
        cout << "Reduce Task is in progress on slave " << j << "..." << endl << endl;
        string request = "reduce";
        availableServers[j] = false;
        string response = slaves[j].ReducerReq(request);
        reduceDone = true;
        break;
      }
    }
    if(reduceDone == true){
      break;
    }
  }
}


float lineCount = 0;
string readFileFromHdfs(string hdfsHost, int hdfsPort, string hdfsFilePath) {
    hdfsFS fs = hdfsConnect(hdfsHost.c_str(), hdfsPort);

    hdfsFile file = hdfsOpenFile(fs, hdfsFilePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        cout << "Failed to open file in HDFS" << endl;
        return "";
    }

    const int bufferSize = 1024;
    char buffer[bufferSize];
    string filecontents;
    int bytesRead = 0;
    while ((bytesRead = hdfsRead(fs, file, buffer, bufferSize)) > 0) {
        filecontents.append(buffer, bytesRead);
    }
    hdfsCloseFile(fs, file);

    string noPunFilecontents;
    for (char c : filecontents) {
        if (isalnum(c) || isspace(c)) {
            noPunFilecontents += tolower(c);
        }
    }

    for (char c : noPunFilecontents) {
        if (c == '\n') {
            lineCount++;
        }
    }

    hdfsDisconnect(fs);
    return noPunFilecontents;
}

void readFile(){
  // string path = "/home/rafay/grpc/examples/cpp/helloworld/input.txt";
  // ifstream file(path);
  // if (!file.is_open()) {
  //     cout << "File Not Found." << endl;
  //     return;
  // }
  // float lineCount = 0;
  // float linesPerT;
  // string line;
  // while (getline(file, line)) {
  //     transform(line.begin(), line.end(), line.begin(), ::tolower);
  //     string noPunLine;
  //     for (char c : line) {
  //         if (isalnum(c) || isspace(c)) {
  //             noPunLine += c;
  //         }
  //     }
  //     filecontents += noPunLine + '\n';
  //     lineCount++;
  // }
  // file.close();

  string hdfsHost = "localhost";
  int hdfsPort = 9000;
  string hdfsFilePath = "/Assignment5/input.txt";

  string filecontents = readFileFromHdfs(hdfsHost, hdfsPort, hdfsFilePath);
  float linesPerT;
  linesPerT = ceil(lineCount / 4);
  // cout << "Lines Per Task : " << ceil(linesPerT) << endl;
  linesPerTask.resize(0);
  for(int i=0;i<3;i++){
    linesPerTask.push_back(linesPerT);
  }
  linesPerTask.push_back(lineCount-(3*linesPerT));
  
  // for(int i=0;i<4;i++){
  //   cout << "Line per task " << i << " : " << linesPerTask[i] << endl;
  // }
  // cout << filecontents << endl;

  int startLine = 0;
  for (int i = 0; i < 4; i++) {
    int endLine = startLine + linesPerTask[i] - 1;
    int rand=0;
    while (endLine < filecontents.size()) {
      if(filecontents[endLine] == '\n'){
        rand++;
      }
      if(rand == linesPerTask[i]){
        break;
      }
      endLine++;
    }
    taskStrings[i] = filecontents.substr(startLine, endLine - startLine + 1);
    startLine = endLine + 1;
  }
  
  // for (int i = 0; i < 4; i++) {
  //   cout << "Task " << i << ":\n" << taskStrings[i] << endl;
  // }

}

int main(int argc, char** argv) {
  
  setenv("CLASSPATH", "/home/rafay/hadoop-3.2.3/etc/hadoop:/home/rafay/hadoop-3.2.3/share/hadoop/common/:/home/rafay/hadoop-3.2.3/share/hadoop/common/lib/:/home/rafay/hadoop-3.2.3/share/hadoop/hdfs/:/home/rafay/hadoop-3.2.3/share/hadoop/hdfs/lib/:/home/rafay/hadoop-3.2.3/share/hadoop/mapreduce/:/home/rafay/hadoop-3.2.3/share/hadoop/mapreduce/lib/", 1);
  vector<string> slavePorts = {"localhost:10000","localhost:10001","localhost:10002","localhost:10003"};  //hardcoded slave portnumbers
  for (int i=0;i<4;i++)
  {
    slaves.push_back(grpc::CreateChannel(slavePorts[i], grpc::InsecureChannelCredentials()));       //initialization of Channels
  }


  readFile();  
  thread taskThread = thread(taskAssignment);
  thread heartThread = thread(HeartBeat);

  heartThread.join();
  taskThread.join();

  return 0;
}