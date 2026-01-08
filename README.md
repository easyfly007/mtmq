# EmirMtmqMgr

**EmirMtmqMgr** (Emir Multi-Task Multi-Queue Manager) is a C++98/03-compatible thread pool library for managing parallel task execution with flexible execution modes and allocation strategies.

## Features

- **C++98/03 Compatible**: Uses `pthread` instead of C++11 features, ensuring compatibility with older compilers
- **Three Execution Modes**:
  - **TD (Top-Down)**: Execute top job first, then parallel leaf jobs
  - **BU (Bottom-Up)**: Execute leaf jobs in parallel first, then top job
  - **RD (Random)**: Execute all jobs randomly in parallel
- **Three Allocation Strategies**:
  - **STATIC**: Pre-allocate jobs to threads (default)
  - **DYNAMIC**: Threads pull jobs from a shared queue
  - **STEAL**: Threads execute assigned jobs, then steal uncompleted jobs from other threads
- **Thread-Safe Debug Output**: Buffered debug output that flushes only on `std::endl` or `std::flush`
- **Shared Arguments**: Pass global parameters or pointers to all threads
- **Task-Based Design**: User-defined tasks inherit from base classes and implement `run()` method
- **Job Reusability**: Jobs are set once and can be reused across multiple task runs

## Architecture

### Core Concepts

- **Job**: User-defined work unit (stored as `void*` pointer)
  - **Top Job**: Single job that runs before/after leaf jobs (depending on mode)
  - **Leaf Jobs**: Multiple jobs that run in parallel
- **Task**: User-defined execution logic (inherits from `EmirMtmqTask`)
  - Determines execution mode (TD/BU/RD)
  - Implements `run(void* job, EmirMtmqArg* arg)` method
- **Argument**: Shared data structure (inherits from `EmirMtmqArg`)
  - Passed to all task executions
  - Used for global parameters or pointers

### Class Hierarchy

```
EmirMtmqTask (base class)
├── EmirMtmqRDtask (Random mode)
├── EmirMtmqTDtask (Top-Down mode)
└── EmirMtmqBUtask (Bottom-Up mode)

EmirMtmqArg (base class for shared arguments)
```

## Requirements

- C++ compiler supporting C++98/03 standard (g++, clang++)
- `pthread` library
- Unix-like environment (Linux, macOS, or Windows with Git Bash/WSL)

## Quick Start

### 1. Include Headers

```cpp
#include "emirMtmqMgr.h"
#include "emirMtmqTask.h"
#include "emirMtmqArg.h"
```

### 2. Define Your Job Type

```cpp
// Example: Simple job with ID
struct MyJob {
    int job_id;
    // ... other job data
};
```

### 3. Define Your Argument Type (Optional)

```cpp
class MyArg : public EmirMtmqArg {
public:
    int global_param;
    void* shared_pointer;
    // ... other shared data
};
```

### 4. Define Your Task Class

```cpp
class MyRDTask : public EmirMtmqRDtask {
public:
    MyRDTask() : EmirMtmqRDtask("MyRDTask") {}  // Set task name
    
    virtual void run(void* job, EmirMtmqArg* arg) {
        MyJob* my_job = static_cast<MyJob*>(job);
        MyArg* my_arg = static_cast<MyArg*>(arg);
        
        // Execute your work here
        std::cout << "Executing job " << my_job->job_id << std::endl;
        // ... your task logic
    }
};
```

### 5. Use EmirMtmqMgr

```cpp
// Create manager with 4 worker threads, enable debug output
EmirMtmqMgr mgr(4, 1);

// Create jobs
MyJob top_job;
top_job.job_id = 0;
std::vector<MyJob> leaf_jobs(10);
for (int i = 0; i < 10; ++i) {
    leaf_jobs[i].job_id = i + 1;
}

// Set jobs
mgr.setTopJob(&top_job);
for (size_t i = 0; i < leaf_jobs.size(); ++i) {
    mgr.addLeafJob(&leaf_jobs[i]);
}

// Set shared argument (optional)
MyArg arg;
arg.global_param = 42;
mgr.setArgument(&arg);

// Start thread pool (must be called after setting jobs)
mgr.start();

// Create and run task
MyRDTask task;
mgr.run(&task);  // Automatically waits for completion

// Run another task with different mode
MyTDTask td_task;
mgr.run(&td_task);

// Cleanup
mgr.shutdown();
```

## API Reference

### EmirMtmqMgr

#### Constructor

```cpp
explicit EmirMtmqMgr(size_t threadCount, int debug = 0);
```

- `threadCount`: Number of worker threads to create
- `debug`: Debug level (>=1 for basic debug output)

#### Job Management

```cpp
void setTopJob(void* job);
void addLeafJob(void* job);
```

Set the top job and add leaf jobs. Jobs are stored as `void*` pointers and remain unchanged across multiple `run()` calls.

#### Argument Management

```cpp
void setArgument(EmirMtmqArg* arg);
```

Set shared argument that will be passed to all task executions.

#### Thread Management

```cpp
void start();
```

Start the thread pool. Must be called after `setTopJob()` and `addLeafJob()` have been called. This function pre-allocates jobs for all three execution modes (TD, BU, RD).

```cpp
void shutdown();
```

Stop the thread pool and wait for all threads to finish.

#### Execution

```cpp
void run(EmirMtmqTask* task);
```

Execute a task. The function:
- Determines execution mode from `task->getMode()`
- Executes all jobs using `task->run(job, arg)`
- Automatically waits for all tasks to complete
- Automatically clears allocation state (jobs remain unchanged)

#### Status Queries

```cpp
size_t getThreadCount() const;
size_t getLeafJobCount() const;
bool hasTopJob() const;
bool isRunning() const;
```

### EmirMtmqTask

#### Base Class

```cpp
class EmirMtmqTask {
public:
    EmirMtmqTask(const std::string& name = "");
    virtual ~EmirMtmqTask();
    
    virtual void run(void* job, EmirMtmqArg* arg) = 0;
    virtual ExecutionMode getMode() const = 0;
    
    const std::string& getName() const;
    void setName(const std::string& name);
};
```

#### Derived Classes

```cpp
class EmirMtmqRDtask : public EmirMtmqTask;  // Random mode
class EmirMtmqTDtask : public EmirMtmqTask;  // Top-Down mode
class EmirMtmqBUtask : public EmirMtmqTask;  // Bottom-Up mode
```

Users should inherit from one of these classes and implement `run(void* job, EmirMtmqArg* arg)`.

### EmirMtmqArg

```cpp
class EmirMtmqArg {
public:
    virtual ~EmirMtmqArg() {}
};
```

Base class for shared arguments. Users should inherit from this class to define their own argument types.

## Execution Modes

### TD (Top-Down) Mode

1. Main thread executes top job first
2. After top job completes, slave threads execute leaf jobs in parallel
3. All threads wait for all leaf jobs to complete

**Use Case**: When top job produces data needed by leaf jobs.

### BU (Bottom-Up) Mode

1. Slave threads execute leaf jobs in parallel first
2. After all leaf jobs complete, main thread executes top job
3. All threads wait for top job to complete

**Use Case**: When leaf jobs produce data needed by top job.

### RD (Random) Mode

1. All jobs (top + leaf) are randomly distributed to all threads
2. All threads execute their assigned jobs in parallel
3. All threads wait for all jobs to complete

**Use Case**: When jobs are independent and can run in any order.

## Allocation Strategies

### STATIC Allocation (Default)

- Jobs are pre-allocated to threads at `start()` time
- Each thread executes only its assigned jobs
- Most efficient for balanced workloads

### DYNAMIC Allocation

- Jobs are placed in a shared queue
- Threads pull jobs from the queue as they become available
- Better load balancing for uneven workloads
- Top job handling respects execution mode (TD/BU/RD)

### STEAL Allocation

- Jobs are initially pre-allocated (like STATIC)
- When a thread finishes its assigned jobs, it can "steal" uncompleted jobs from other threads
- Completed jobs are marked as "done" to prevent duplicate execution
- Better load balancing while maintaining locality
- Top job handling respects execution mode (TD/BU/RD)

## Debug Output

The library provides thread-safe debug output through `emir_debug`:

```cpp
#include "emirMtmqDebug.h"

emir_debug << "[Thread " << thread_id << "] ";
emir_debug << "Task: " << task_name << " ";
emir_debug << "Job: " << job_id << std::endl;  // Flushes here
```

**Features**:
- Thread-safe: Each thread has its own buffer
- Buffered: Output is accumulated until `std::endl` or `std::flush`
- Atomic: Each flush outputs a complete line

Enable debug output by setting `debug >= 1` in the constructor.

## File Structure

```
mtmq/
├── emirMtmqMgr.h          # Manager class header
├── emirMtmqMgr.cc         # Manager class implementation
├── emirMtmqTask.h         # Task class hierarchy header
├── emirMtmqTask.cc        # Task class implementation
├── emirMtmqArg.h          # Argument base class
├── emirMtmqDebug.h        # Debug output utility header
├── emirMtmqDebug.cc       # Debug output utility implementation
├── README.md              # This file
└── unittest/              # Unit tests
    ├── test_mtmqMgr.cc    # Test program
    ├── runme.sh           # Build and run script
    └── README.md          # Test documentation
```

## Testing

### Run Unit Tests

```bash
cd unittest
chmod +x runme.sh
./runme.sh
```

### Manual Compilation

```bash
g++ -std=c++98 -Wall -Wextra -O2 -pthread \
    emirMtmqMgr.cc emirMtmqTask.cc emirMtmqDebug.cc \
    your_program.cc -o your_program

./your_program
```

## Important Notes

1. **Job Lifetime**: Jobs must remain valid throughout the lifetime of `EmirMtmqMgr`. Do not delete jobs while the manager is running.

2. **Task Lifetime**: Task objects must remain valid during `run()` execution. Do not delete tasks while they are being executed.

3. **Thread Safety**: The library is thread-safe, but user-defined `run()` methods must be thread-safe if they access shared data.

4. **Order of Operations**:
   - Create `EmirMtmqMgr`
   - Set jobs (`setTopJob()`, `addLeafJob()`)
   - Set argument (optional, `setArgument()`)
   - Call `start()` (pre-allocates jobs and creates threads)
   - Call `run()` multiple times with different tasks
   - Call `shutdown()` when done

5. **Job Reusability**: Jobs are set once and can be reused across multiple `run()` calls. The `run()` function automatically clears allocation state but preserves job data.

6. **Execution Mode**: Execution mode is determined by the task type (`EmirMtmqRDtask`, `EmirMtmqTDtask`, `EmirMtmqBUtask`), not by the manager.

7. **Allocation Mode**: Currently, allocation mode is set internally. STATIC allocation is the default.

## Example: Complete Program

```cpp
#include "emirMtmqMgr.h"
#include "emirMtmqTask.h"
#include "emirMtmqArg.h"
#include <iostream>
#include <vector>

// Define job
struct Job {
    int id;
    Job(int i) : id(i) {}
};

// Define argument
class MyArg : public EmirMtmqArg {
public:
    int multiplier;
    MyArg(int m) : multiplier(m) {}
};

// Define RD task
class MyRDTask : public EmirMtmqRDtask {
public:
    MyRDTask() : EmirMtmqRDtask("MyRDTask") {}
    
    virtual void run(void* job, EmirMtmqArg* arg) {
        Job* j = static_cast<Job*>(job);
        MyArg* a = static_cast<MyArg*>(arg);
        
        int result = j->id * (a ? a->multiplier : 1);
        std::cout << "Job " << j->id << " -> " << result << std::endl;
    }
};

int main() {
    // Create manager
    EmirMtmqMgr mgr(4, 1);
    
    // Create jobs
    Job top_job(0);
    std::vector<Job> leaf_jobs;
    for (int i = 1; i <= 10; ++i) {
        leaf_jobs.push_back(Job(i));
    }
    
    // Set jobs
    mgr.setTopJob(&top_job);
    for (size_t i = 0; i < leaf_jobs.size(); ++i) {
        mgr.addLeafJob(&leaf_jobs[i]);
    }
    
    // Set argument
    MyArg arg(2);
    mgr.setArgument(&arg);
    
    // Start threads
    mgr.start();
    
    // Run task
    MyRDTask task;
    mgr.run(&task);
    
    // Cleanup
    mgr.shutdown();
    
    return 0;
}
```

## License

This project is provided as-is for personal and educational use.

## Contributing

Contributions are welcome! Please ensure code follows C++98/03 standards and maintains compatibility with the existing API.

