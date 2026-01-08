#!/bin/bash

# ============================================================================
# EmirMtmqMgr Test Runner Script
# ============================================================================
# This script compiles all source files and runs the test program
# ============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Compiler and flags
CXX=g++
CXXFLAGS="-std=c++98 -Wall -Wextra -O2 -pthread -I.."
TARGET=test_mtmqMgr

# Source files from parent directory
PARENT_SOURCES="../emirMtmqMgr.cc ../emirMtmqTask.cc ../emirMtmqDebug.cc"

# Test source file in current directory
TEST_SOURCE="test_mtmqMgr.cc"

# All sources to compile
SOURCES="${PARENT_SOURCES} ${TEST_SOURCE}"

echo "=========================================="
echo "  EmirMtmqMgr Test Compiler & Runner"
echo "=========================================="
echo ""

# Clean previous build
echo -e "${YELLOW}Cleaning previous build...${NC}"
rm -f ${TARGET} *.o
echo ""

# Compile
echo -e "${YELLOW}Compiling sources...${NC}"
${CXX} ${CXXFLAGS} -o ${TARGET} ${SOURCES}

# Check if compilation was successful
if [ $? -ne 0 ]; then
    echo -e "${RED}Compilation failed!${NC}"
    exit 1
fi

echo -e "${GREEN}Compilation successful!${NC}"
echo ""

# Run test
echo "=========================================="
echo "  Running Tests"
echo "=========================================="
echo ""
./${TARGET}

# Save exit code
TEST_EXIT_CODE=$?

echo ""
if [ ${TEST_EXIT_CODE} -eq 0 ]; then
    echo -e "${GREEN}=========================================="
    echo "  All tests passed! ✓"
    echo -e "==========================================${NC}"
else
    echo -e "${RED}=========================================="
    echo "  Tests failed!"
    echo -e "==========================================${NC}"
fi

exit ${TEST_EXIT_CODE}

