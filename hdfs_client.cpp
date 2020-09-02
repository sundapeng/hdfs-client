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
    LOG_ERROR << "connect error: " << ww::ErrStr(errno);
  } else {
    LOG_INFO << "connect ok: " << name_node << ":"
             << std::to_string(hdfs_port_);
  }
  return fs;
}

int HdfsClient::CopyToLocal(const std::string &hdfs_path,
                            const std::string &save_path, bool rename,
                            int retry) {
  int res = -1;
  auto real_hdfs_path = ww::RemoveLastSlash(hdfs_path);
  auto real_save_path = ww::RemoveLastSlash(save_path);

  if (!rename) {
    real_save_path =
        ww::AppendLastSlash(real_save_path) + ww::GetLastDir(real_hdfs_path);
  }

  bool need_over_write = ww::Exist(real_save_path) == 0;

  while (retry-- >= 0) {
    if (!fs_) {
      sleep(1);
      LOG_ERROR << "invalid fs, try to reconnect";
      InitFs();
      continue;
    }

    if (need_over_write) {
      hdfsFileInfo *file_info = hdfsGetPathInfo(fs_, real_hdfs_path.c_str());

      if (!file_info) {
        sleep(1);
        LOG_ERROR << "get file info error: " << ww::ErrStr(errno)
                  << ", path: " << hdfs_path;
        continue;
      } else {
        res = OverWrite(real_hdfs_path, real_save_path, file_info->mKind);
      }
    } else {
      res = Create(real_hdfs_path, real_save_path);
    }

    if (res == 0) {
      break;
    }
  }

  return res;
}

int HdfsClient::OverWrite(const std::string &hdfs_path,
                          const std::string &save_path, tObjectKind m_kind) {
  int res = -1;

  if (m_kind == tObjectKind::kObjectKindFile) {
    LOG_INFO << "hdfs rename " << hdfs_path << ", to " << save_path;
    ww::Rm(save_path);
    res = Create(hdfs_path, save_path);
  } else if (m_kind == tObjectKind::kObjectKindDirectory) {
    int count = -1;
    hdfsFileInfo *info_ptr_list =
        hdfsListDirectory(fs_, hdfs_path.c_str(), &count);

    if (!info_ptr_list) {
      LOG_ERROR << "list dir error: " << ww::ErrStr(errno)
                << ", path: " << hdfs_path;
      return res;
    }

    LOG_INFO << "hdfs rename " << hdfs_path << ", to " << save_path
             << ", file/dir count: " << count;

    if (count > 0) {
      auto info_ptr = info_ptr_list;
      for (int i = 0; i < count; ++i, ++info_ptr) {
        if (!info_ptr->mName) {
          LOG_ERROR << "invalid mName";
          continue;
        }
        auto new_hdfs_path = std::string(info_ptr->mName);
        auto new_save_path =
            ww::AppendLastSlash(save_path) + ww::GetLastDir(new_hdfs_path);
        if (info_ptr->mKind == tObjectKind::kObjectKindFile) {
          ww::Rm(new_save_path);
          res = Create(new_hdfs_path, new_save_path);
        } else if (info_ptr->mKind == tObjectKind::kObjectKindDirectory) {
          if (ww::Exist(new_save_path) == 0) {
            res = OverWrite(new_hdfs_path, new_save_path, info_ptr->mKind);
          } else {
            res = Create(new_hdfs_path, new_save_path);
          }
        } else {
          LOG_ERROR << "unkown mKind";
        }
      }
    } else {
      LOG_ERROR << "list dir count 0, hdfs path: " << hdfs_path;
    }

    hdfsFreeFileInfo(info_ptr_list, count);
  } else {
    LOG_ERROR << "unkown mKind";
  }

  if (res != 0) {
    LOG_ERROR << "rename to local error: " << ww::ErrStr(errno)
              << ", hdfs path: " << hdfs_path << ", save path: " << save_path;
  }

  return res;
}

int HdfsClient::Create(const std::string &hdfs_path,
                       const std::string &save_path) {
  LOG_INFO << "hdfs load " << hdfs_path << ", to " << save_path;

  int res = hdfsCopy(fs_, hdfs_path.c_str(), local_fs_, save_path.c_str());
  if (res != 0) {
    LOG_ERROR << "load to local error: " << ww::ErrStr(errno)
              << ", hdfs path: " << hdfs_path << ", save path: " << save_path;
  }

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