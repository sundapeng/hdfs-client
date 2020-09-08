#include <iostream>
#include <string>
#include <vector>

#include "hdfs_client.h"

int main() {
  auto client =
      HdfsClient(std::vector<std::string>{"emr-cluster"});

  client.CopyToLocal(
      "/apps",
      "/root", false, 0);

  return 0;
}
