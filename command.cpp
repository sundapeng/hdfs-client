#include "command.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace ww {

enum class CMD { MV, EXI, MKDIR, RM };

const std::map<CMD, std::string> CMD_MAP{
    {CMD::EXI, "access"}, {CMD::MKDIR, "mkdir"}, {CMD::RM, "rm"}};

int RmDir(const std::string &path) {
  DIR *d = opendir(path.c_str());
  size_t path_len = path.size();
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;
    while (r == 0 && (p = readdir(d))) {
      int r2 = -1;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." as we don't want to recurse on them. */
      if (strcmp(p->d_name, ".") == 0 || strcmp(p->d_name, "..") == 0) {
        continue;
      }

      len = path_len + strlen(p->d_name) + 2;
      buf = (char *)malloc(len);

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path.c_str(), p->d_name);
        if (stat(buf, &statbuf) == 0) {
          if (S_ISDIR(statbuf.st_mode))
            r2 = RmDir(buf);
          else
            r2 = unlink(buf);
        }
        free(buf);
      }
      r = r2;
    }
    closedir(d);
  }

  if (r == 0) {
    r = rmdir(path.c_str());
  }

  return r;
}

int RmForce(const std::string &path) {
  struct stat buffer;
  int res = stat(path.c_str(), &buffer);
  if (res == 0) {
    res = S_ISDIR(buffer.st_mode) ? RmDir(path) : remove(path.c_str());
  }

  return res;
}

int RunCmd(const std::string &path, CMD type, int retry = 0) {
  if (CMD_MAP.count(type) == 0) {
    LOG_ERROR << "invalid cmd type";
    return -1;
  }
  if (retry < 0) {
    return -1;
  }

  auto real_path = RemoveLastSlash(path);

  int res = -1;
  while (retry-- >= 0) {
    switch (type) {
      case CMD::MKDIR: {
        LOG_INFO << "mkdir " << real_path;
        res = mkdir(real_path.c_str(), S_IRWXU);
        break;
      }
      case CMD::RM: {
        LOG_INFO << "rm " << real_path;
        res = RmForce(real_path);
        break;
      }
      case CMD::EXI: {
        //        LOG_INFO << "access " << real_path;
        res = access(real_path.c_str(), R_OK);
        return res;
      }
      default: {
        LOG_INFO << "invalid cmd type";
        return res;
      }
    }

    if (res == 0) {
      break;
    } else {
      LOG_ERROR << CMD_MAP.at(type) << " error, code: " << res
                << ", err msg: " << ErrStr(errno) << ", path: " << real_path;
      sleep(1);
    }
  }

  return res;
}

int RunCmd(const std::string &src, const std::string &dst, CMD type,
           int retry = 0) {
  if (CMD_MAP.count(type) == 0) {
    LOG_ERROR << "invalid cmd type";
    return -1;
  }

  auto real_src = RemoveLastSlash(src);
  auto real_dst = RemoveLastSlash(dst);

  int res = -1;
  while (retry-- >= 0) {
    switch (type) {
      case CMD::MV: {
        LOG_INFO << "rename " << real_src << " to " << real_dst;
        res = rename(real_src.c_str(), real_dst.c_str());
        break;
      }
      default: {
        LOG_INFO << "invalid cmd type";
      }
    }

    if (res == 0) {
      break;
    } else {
      LOG_ERROR << CMD_MAP.at(type) << " error, code: " << res
                << ", err msg: " << ErrStr(errno) << ", src: " << real_src
                << ", dst: " << real_dst;
      sleep(1);
    }
  }

  return res;
}

std::string GetLastDir(const std::string &path) {
  std::string res = path;

  if (path.empty()) {
    return res;
  }

  auto pos = path.find_last_of('/');
  if (pos == std::string::npos) {
    return res;
  }
  if (++pos >= path.size()) {
    return res;
  }

  res = path.substr(pos);
  return res;
}

std::string GetParentDir(const std::string &path) {
  std::string res = path;

  if (path.empty()) {
    return res;
  }

  auto pos = path.find_last_of('/');
  if (pos == std::string::npos) {
    return res;
  }
  if (pos == 0) {
    return res;
  }

  res = path.substr(0, pos);
  return res;
}

int InitPath(const std::string &path) {
  Rm(path);
  return MkDir(path);
}

int MkDir(const std::string &path) {
  if (Exist(path) == 0) {
    return 0;
  }
  return RunCmd(path, CMD::MKDIR);
}

int Rm(const std::string &path) {
  if (Exist(path) != 0) {
    return 0;
  }

  static const std::string INVALID_PATH = "/home/worker/";
  if (INVALID_PATH.find(path) == 0) {
    LOG_ERROR << "invalid path to rm: " << path;
    return -1;
  }

  return RunCmd(path, CMD::RM);
}

int Mv(const std::string &src, const std::string &dst) {
  int res = Exist(src);
  if (res != 0) {
    LOG_INFO << "mv src not exist: " << src;
    return 0;
  }

  return RunCmd(src, dst, CMD::MV);
}

int Exist(const std::string &path) { return RunCmd(path, CMD::EXI); }

std::string ErrStr(int err_no) {
  char *e = strerror(errno);
  return e ? e : "no error message";
}

std::string AppendLastSlash(const std::string &path) {
  auto res = path;
  if (!res.empty() && res.back() != '/') {
    res += "/";
  }
  return res;
}

std::string RemoveLastSlash(const std::string &path) {
  auto res = path;
  if (!path.empty() && path.back() == '/') {
    res.pop_back();
  }
  return res;
}

}  // namespace ww