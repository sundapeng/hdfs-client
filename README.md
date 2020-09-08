yum install -y java

export CLASSPATH=/usr/lib/hadoop-current/bin/:`hadoop classpath --glob`

g++ --std=c++11 ./test.cpp ./command.cpp ./hdfs_client.cpp  -I/usr/lib/hadoop-current/include/  -L/usr/lib/hadoop-current/lib/native/  -L/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/amd64/server  -lhdfs -ljvm -lpthread

export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/amd64/server:/usr/lib/hadoop-current/lib/native/

./a.out
