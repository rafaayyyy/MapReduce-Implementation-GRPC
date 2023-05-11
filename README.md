# MapReduce-Implementation-GRPC
 
## Setup:

For this project, I used the Ubuntu-based system. The dependencies and links with other files were used as it is from one of the examples given. I used the '`Hello World`' example project to implement my master-slave implementation. The location of the proto file and other dependencies were predetermined and were used as it is. Cmake was used to build and run the project.

The commands to run my project from the root directory are as follows.

- mkdir -p cmake/build

- pushd cmake/build

- cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..

- make -j 4

After this, we can run our slaves (`./greeter_server.cc`) and master (`./greeter_client.cc`) accordingly.

## Explanation:

Hadoop File System API: Hadoop File System was installed, and its API was used to read and write files directly from the code. The original file named "`input.txt`" is read from the Hadoop file system and divided for tasks among slaves for the Map phase. Additionally, the final reduced output is also saved on HDFS with the name "`ReducedOutput.txt`".

`greeter_client.cc` (Master): The main implementation for all the controlling of slaves and task assignments and management was to be done here. First, we hard-code the ports that we will create slaves on. In my case, port numbers 10000, 10001, 10002, and 10003 were used. Control Interval and Timeout Interval were set as 1 and 4 seconds respectively. Chrono library was used for time intervals. After connecting the master and slaves using `grpc::CreateChannel`, the master indefinitely pings its slaves on a separate thread in a while true loop. For our Heartbeat function, if a response was not made in our timeout interval, that slave node was deemed unresponsive. Or in another case, if a slave returned an empty string (which essentially means that specific slave code is not running), the master declared that slave as offline. The request is made to all the slaves after every second, however, we can also change it on runtime for every next iteration i.e., change the control interval time. The response also determined the availability of slave machines and the completion of map tasks. Two functions namely: `HeartBeat()` and `taskAssignment()` were created. To keep a record of online machines, available machines, tasks assignments, and completion, as well as which task was assigned to which machine, multiple global vectors were also created. Finally, the master outputs the interface which shows all the statuses of slave nodes as well as tasks. It also shows us which task is assigned to which machine and in case of failure during a task, it is reassigned to any other available online machine. The master keeps track of all the completion of map tasks and after they are done, we move to reduce phase. The master requests any of the slaves for reducer function and further processing is done in that slave. It also shows which slave is ultimately performing the Reducer Function.

`greeter_server.cc` (Slaves): The implementation of slaves was relatively easy. A port number was taken as input from the user which was to be checked for response by the master. A Mapper function was created that just works like the original `SayHello` (ping) function that returns the percentage done for a task. A sleep function of 5 seconds was used. The socket was set to a listening mode for the request sent by the master. If a slave is online and it successfully gets the map task sent by the master, it replies to the string "request" sent by the master with a string containing "0" or "100" where they represent task incomplete, and task complete respectively. If it is offline, nothing occurs, the master declares the node as unresponsive, and no map task is assigned. Mapper and Reducer Functions were defined in the slave code. Each of the Mapper Functions performs subsequent processes as in-place write for the number of occurrences for every word. As the input is divided into parts, there is a separate intermediary file created on the local file system namely "`MapOutputTask0.txt`" and so on using unordered pair of string and integer. For Reducer Function (performed once by one machine only), the text is read from all the files on the local file system and then uses another unordered pair to reduce and give the final output. After the output has been calculated, it is saved on HDFS using the relevant command in overwrite mode. Also, it prompts the user to enter a number to print N number of words having the most occurrences. It then shows the output on the terminal.

`helloworld.proto`: This file contains our basic service and other messages for the communication between our server and client. Command and path locations for the proto file were already predetermined by the makefile provided by the original Hello World Package. This file introduced new services and messages for our Map and Reduce Tasks and their subsequent requests and replies.