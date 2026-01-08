#ifndef EMIRMTMQTASK_H
#define EMIRMTMQTASK_H

#include <string>

// Forward declaration
class EmirMtmqArg;

// ============================================================================
// Execution mode enumeration
// ============================================================================
enum ExecutionMode {
    TD,  // Top-Down: Main thread runs top job, then slave threads run leaf jobs
    BU,  // Bottom-Up: Slave threads run leaf jobs, then main thread runs top job
    RD   // Random: All threads randomly run all tasks
};

// ============================================================================
// EmirMtmqTask base class
// ============================================================================
class EmirMtmqTask {
public:
    // Constructor: Initialize task with optional name
    EmirMtmqTask(const std::string& name = "");
    
    virtual ~EmirMtmqTask();
    
    // Execute task (implemented by subclasses for specific work)
    // job: Current Job to execute (void* type, user-defined Job type)
    // arg: Shared argument (EmirMtmqArg* type, can be NULL if not set)
    virtual void run(void* job, EmirMtmqArg* arg) = 0;
    
    // Get task execution mode
    virtual ExecutionMode getMode() const = 0;
    
    // Get task name (for debug output)
    const std::string& getName() const;
    
    // Set task name (for debug output)
    void setName(const std::string& name);

protected:
    std::string _name;  // Task name for debug output
};

// ============================================================================
// EmirMtmqRDtask - RD mode task base class
// ============================================================================
class EmirMtmqRDtask : public EmirMtmqTask {
public:
    // Constructor: Initialize RD task with optional name
    EmirMtmqRDtask(const std::string& name = "");
    
    virtual ~EmirMtmqRDtask();
    virtual ExecutionMode getMode() const;
    // run() function is implemented by subclasses
};

// ============================================================================
// EmirMtmqTDtask - TD mode task base class
// ============================================================================
class EmirMtmqTDtask : public EmirMtmqTask {
public:
    // Constructor: Initialize TD task with optional name
    EmirMtmqTDtask(const std::string& name = "");
    
    virtual ~EmirMtmqTDtask();
    virtual ExecutionMode getMode() const;
    // run() function is implemented by subclasses
};

// ============================================================================
// EmirMtmqBUtask - BU mode task base class
// ============================================================================
class EmirMtmqBUtask : public EmirMtmqTask {
public:
    // Constructor: Initialize BU task with optional name
    EmirMtmqBUtask(const std::string& name = "");
    
    virtual ~EmirMtmqBUtask();
    virtual ExecutionMode getMode() const;
    // run() function is implemented by subclasses
};

// ============================================================================
// Usage instructions
// ============================================================================
// Users need to inherit from EmirMtmqRDtask, EmirMtmqTDtask, or EmirMtmqBUtask, and implement run(void* job, EmirMtmqArg* arg) function
// In the run() function, convert the void* parameter to user-defined Job* type
// The arg parameter is a shared argument that can be used to pass global parameters or pointers
// The _name member can be used for debug output to track task progress
//
// Example:
//   class MyJob { ... };
//   class MyArg : public EmirMtmqArg {
//   public:
//       int some_parameter;
//       void* some_pointer;
//   };
//   class MyRDTask : public EmirMtmqRDtask {
//   public:
//       MyRDTask() : EmirMtmqRDtask("MyRDTask") {}  // Set task name in constructor
//       // Or: MyRDTask() { setName("MyRDTask"); }
//       
//       virtual void run(void* job, EmirMtmqArg* arg) {
//           MyJob* my_job = static_cast<MyJob*>(job);
//           MyArg* my_arg = static_cast<MyArg*>(arg);
//           // Use _name for debug output
//           // emir_debug << "Task [" << _name << "] executing job " << job_id << std::endl;
//           // Execute specific work using my_job and my_arg
//       }
//   };
// ============================================================================

#endif // EMIRMTMQTASK_H

