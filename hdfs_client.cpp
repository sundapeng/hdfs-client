#include "hdfs_client.h"

#include <string.h>
#include <unistd.h>

int HdfsClient::setNameNodes(const std::vector<std::string> &name_nodes) {
  if (name_nodes_ == name_nodes) {
    return 0;
  }
  name_nodes_ = name_nodes;
  return 1;
}

void HdfsClient::Init() {
  hdfs_port_ = 8020;
  domain_port_ = 50070;
  local_fs_ = hdfsConnectNewInstance(NULL, 0);
}

HdfsClient::HdfsClient() { Init(); }

HdfsClient::HdfsClient(const std::vector<std::string> &name_nodes)
    : name_nodes_(name_nodes) {
  Init();
  int res = InitFs();
  if (res != 0) {
    LOG_ERROR << "init fs error";
  }
}

int HdfsClient::InitFs() {
  static std::string url =
      ":" + std::to_string(domain_port_) +
      "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus";
  static std::string STATE = "State";
  static std::string ACTIVE = "active";

  for (auto &name_node : name_nodes_) {
    if (name_node.empty()) {
      LOG_ERROR << "empty name node";
      continue;
    }
    try {
      std::string rsp;
      HttpGet(name_node + url, rsp, 2000);
      if (!rsp.empty()) {
        std::string state;
        if (GetJsonValueStr(rsp, STATE, state)) {
          if (state == ACTIVE) {
            fs_ = Connect(name_node);
            return 0;
          }
        } else {
          LOG_ERROR << "no filed: " << STATE << ", rsp: " << rsp;
        }
      } else {
        LOG_ERROR << "empty http rsp, url: " << name_node + url;
      }
    } catch (const std::exception &e) {
      LOG_ERROR << "init fs exception: " << e.what();
    } catch (...) {
      LOG_ERROR << "init fs unknown error";
    }
  }
  return -1;
}

hdfsFS HdfsClient::Connect(const std::string &name_node) {
  hdfsFS fs = hdfsConnect(name_node.c_str(), hdfs_port_);
  if (!fs) {
    LOG_ERROR << "connect error: " << ErrStr(errno);
  } else {
    LOG_INFO << "connect ok: " << name_node << ":"
             << std::to_string(hdfs_port_);
  }
  return fs;
}

int HdfsClient::CopyToLocal(const std::string &hdfs_path,
                            const std::string &save_path, int retry) {
  int res = -1;
  while (retry-- >= 0) {
    if (!fs_) {
      sleep(1);
      LOG_ERROR << "invalid fs, try to reconnect";
      InitFs();
      continue;
    }

    res = hdfsCopy(fs_, hdfs_path.c_str(), local_fs_, save_path.c_str());
    if (res == 0) {
      break;
    } else {
      LOG_ERROR << "load from hdfs error: " << ErrStr(errno);
      sleep(1);
    }
  }

  // todo: check size

  return res;
}

std::string HdfsClient::Tail(const std::string &url, int n) {
  return PosRead(url, "tail", n, 5);
}

std::string HdfsClient::PosRead(const std::string &url,
                                const std::string &cmd_name, int n, int retry) {
  std::string res;
  FILE *fp = NULL;
  std::string cmd =
      "hdfs dfs -cat " + url + " | " + cmd_name + " -n " + std::to_string(n);
  LOG_INFO << "command: " << cmd;

  while (retry-- >= 0) {
    fp = popen(cmd.c_str(), "r");
    if (fp == NULL) {
      LOG_ERROR << "popen error, null file descriptor";
      sleep(1);
      continue;
    }

    char buffer[1024];
    memset(buffer, 0x00, sizeof(buffer));
    char *p = fgets(buffer, sizeof(buffer), fp);
    if (p == NULL) {
      LOG_ERROR << "no data in file descriptor";
      sleep(1);
      continue;
    } else {
      res = std::string(buffer);
      break;
    }
  }

  pclose(fp);
  return res;
}