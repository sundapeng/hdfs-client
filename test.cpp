#include <iostream>
#include <string>
#include <vector>

#include "hdfs_client.h"

int main() {
  auto client =
      HdfsClient(std::vector<std::string>{"name_node1, name_node2"});

  client.CopyToLocal(
      "/hdfs_path",
      "/local_path");

  return 0;
}
