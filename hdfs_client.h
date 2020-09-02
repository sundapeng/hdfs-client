#ifndef PREDICT_SERVER_HDFS_CLIENT_H
#define PREDICT_SERVER_HDFS_CLIENT_H

#include <hdfs.h>

#include <string>
#include <vector>

#include "command.h"

class HdfsClient {
 public:
  HdfsClient();
  HdfsClient(const std::vector<std::string> &name_nodes);
  int setNameNodes(const std::vector<std::string> &name_nodes);
  int InitFs();
  int CopyToLocal(const std::string &hdfs_path, const std::string &save_path,
                  bool rename, int retry = 2);

  static std::string Tail(const std::string &url, int n);

 private:
  int hdfs_port_;
  int domain_port_;
  hdfsFS fs_;
  hdfsFS local_fs_;
  std::vector<std::string> name_nodes_;

  void Init();
  hdfsFS Connect(const std::string &name_node);
  int Create(const std::string &hdfs_path, const std::string &save_path);
  int OverWrite(const std::string &hdfs_path, const std::string &save_path,
                tObjectKind m_kind);

  // need to add valid json parser
  bool GetJsonValueStr(const std::string &json, const std::string &key,
                       std::string &value) {
    value = "active";
    return true;
  }

  // need to add http client
  int HttpGet(const std::string &url, std::string &rsp, int timeout) {
    rsp = "mock rsp";
    return 0;
  }

  static std::string PosRead(const std::string &url,
                             const std::string &cmd_name, int n, int retry = 3);
};

#endif  // PREDICT_SERVER_HDFS_CLIENT_H
