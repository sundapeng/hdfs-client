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

namespace ww {

int InitPath(const std::string &path);
int MkDir(const std::string &path);
int Mv(const std::string &src, const std::string &dst);
int Rm(const std::string &path);
int Exist(const std::string &path);
std::string AppendLastSlash(const std::string &path);
std::string RemoveLastSlash(const std::string &path);
std::string GetLastDir(const std::string &path);
std::string GetParentDir(const std::string &path);
std::string ErrStr(int err_no);

};  // namespace ww

#endif  // PREDICT_SERVER_COMMAND_H
