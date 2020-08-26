yum install -y java

~~export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.262.b10-0.el7_8.x86_64/jre~~

export CLASSPATH=/opt/hadoop-2.6.0-cdh5.5.0/bin:\`hadoop classpath --glob\`

g++ --std=c++11 ./test.cpp ./command.cpp ./hdfs_client.cpp -I/opt/hadoop-2.6.0-cdh5.5.0/include -L/opt/hadoop-2.6.0-cdh5.5.0/lib/native  -L/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.262.b10-0.el7_8.x86_64/jre/lib/amd64/server -lhdfs -ljvm -lpthread

export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.262.b10-0.el7_8.x86_64/jre/lib/amd64/server:/opt/hadoop-2.6.0-cdh5.5.0/lib/native
