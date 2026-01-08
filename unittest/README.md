# EmirMtmqMgr Unit Test

This directory contains unit tests for EmirMtmqMgr.

## Files

- `test_mtmqMgr.cc` - Main test program
- `runme.sh` - Build and run script
- `README.md` - This file

Note: Source files are located in the parent directory:
- `../emirMtmqMgr.h` / `../emirMtmqMgr.cc` - Manager class implementation
- `../emirMtmqTask.h` / `../emirMtmqTask.cc` - Task class implementation
- `../emirMtmqArg.h` - Argument class definition
- `../emirMtmqDebug.h` / `../emirMtmqDebug.cc` - Debug output utility

## Usage

### On Linux/macOS/Git Bash (Windows):

```bash
chmod +x runme.sh
./runme.sh
```

### On Windows (PowerShell/CMD):

The script requires a bash environment (Git Bash, WSL, or similar).
Alternatively, you can compile manually:

```bash
g++ -std=c++98 -Wall -Wextra -O2 -pthread -o test_mtmqMgr \
    emirMtmqMgr.cc emirMtmqTask.cc emirMtmqDebug.cc test_mtmqMgr.cc

./test_mtmqMgr
```

## Requirements

- C++ compiler supporting C++98 standard (g++, clang++)
- pthread library
- Unix-like environment (for bash script)

## Test Cases

The test program includes the following test cases:

1. TD (Top-Down) mode test
2. BU (Bottom-Up) mode test
3. RD (Random) mode test
4. TD mode - Large task test
5. BU mode - Parallel execution verification
6. RD mode - Random allocation verification
7. Multiple runs with different modes

