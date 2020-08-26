#ifndef PREDICT_SERVER_COMMAND_H
#define PREDICT_SERVER_COMMAND_H

#include <map>
#include <string>

#ifndef LOG_INFO
#include <iostream>
#define LOG_INFO std::cout << std::endl
#endif

#ifndef LOG_ERROR
#include <iostream>
#define LOG_ERROR std::cout << std::endl
#endif

class Command {
 public:
  enum class CMD { MV, EXI, MKDIR, RM };
  static const std::map<CMD, std::string> cmd_map_;

  static int InitPath(const std::string &path);
  static int MkDir(const std::string &path);
  static int Mv(const std::string &src, const std::string &dst);
  static int Rm(const std::string &path);
  static int Exist(const std::string &path);
  static std::string AppendLastSlash(const std::string &path);
  static std::string RemoveLastSlash(const std::string &path);
  static std::string GetLastDir(const std::string &path);
  static std::string GetParentDir(const std::string &path);
  static std::string ErrStr(int err_no);

 private:
  static int RunCmd(const std::string &path, CMD cmd_name, int retry = 0);
  static int RunCmd(const std::string &src, const std::string &dst,
                    CMD cmd_name, int retry = 0);
  static int RmDir(const std::string &path);
};

#endif  // PREDICT_SERVER_COMMAND_H
