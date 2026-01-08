#ifndef EMIRMTMQMGR_H
#define EMIRMTMQMGR_H

#include <vector>
#include <queue>
#include <pthread.h>
#include <cstddef>
#include "emirMtmqTask.h"  // Include Task related class definitions
#include "emirMtmqArg.h"   // Include Argument class definitions

// Forward declaration
class EmirMtmqMgr;

// ============================================================================
// Job forward declaration
// Job is user-defined, EmirMtmqMgr doesn't care about the specific type of Job
// ============================================================================
// Job is a user-defined type, EmirMtmqMgr uses void* to store Job pointers

class EmirMtmqMgr {
public:
    // Constructor: Create specified number of worker threads
    // threadCount: Number of worker threads to create
    // debug: Debug level (>=1 for basic debug output)
    explicit EmirMtmqMgr(size_t threadCount, int debug = 0);
    
    // Destructor: Wait for all tasks to complete and close thread pool
    ~EmirMtmqMgr();
    
    // Disable copy and assignment
    EmirMtmqMgr(const EmirMtmqMgr&);
    EmirMtmqMgr& operator=(const EmirMtmqMgr&);
    
    // === Job setup (Job is user-defined, stored as void*) ===
    // Set top job
    void setTopJob(void* job);
    
    // Add leaf job
    void addLeafJob(void* job);
    
    // === Argument setup ===
    // Set shared argument (shared across all threads)
    // arg: Pointer to EmirMtmqArg object (or user-defined class inheriting from EmirMtmqArg)
    void setArgument(EmirMtmqArg* arg);
    
    // Clear task allocation state (does not clear Job, Job remains unchanged and can be reused)
    // Note: Usually no need to call manually, as run() will automatically clear allocation state
    void clearTasks();
    
    // === Thread management ===
    // Start thread pool (must be called after setTopJob/addLeafJob)
    // This function will pre-allocate all three modes (TD, BU, RD) and create worker threads
    void start();
    
    // === Allocation and execution ===
    // Set the Task type to run and execute
    // task: Task instance, used to determine type and execution mode
    // At runtime, will call Task's run(Job*, EmirMtmqArg*) method for each Job
    // Function will automatically wait for all tasks to complete before returning, and automatically clear allocation state (Job remains unchanged)
    void run(EmirMtmqTask* task);
    
    // Stop thread pool
    void shutdown();
    
    // === Status query ===
    size_t getThreadCount() const;
    size_t getLeafJobCount() const;
    bool hasTopJob() const;
    bool isRunning() const;
    
private:
    // === Internal methods ===
    // Worker thread main loop function (static function for pthread)
    static void* worker_thread(void* arg);
    
    // Worker thread actual execution function
    void worker(size_t thread_id);
    
    // Allocation strategies
    void allocateTD();   // TD mode allocation (saves to _thread_jobs_td)
    void allocateBU();   // BU mode allocation (saves to _thread_jobs_bu)
    void allocateRD();   // RD mode allocation (saves to _thread_jobs_rd)
    void allocateAll();  // Allocate all three modes (called after setTopJob/addLeafJob)
    
    // Execution control (called in main thread)
    void executeTD();    // TD mode execution
    void executeBU();    // BU mode execution
    void executeRD();    // RD mode execution
    
    // Clear task allocation state
    void clearAllocation();
    
    // === Member variables ===
    size_t _thread_count;                        // Number of threads
    std::vector<pthread_t> _workers;             // Worker thread container
    
    // Job storage (Job is user-defined, stored as void*)
    void* _top_job;                               // Top job
    bool _has_top_job;                            // Whether top job is set
    std::vector<void*> _leaf_jobs;                // Leaf jobs list
    
    // Shared argument (shared across all threads)
    EmirMtmqArg* _argument;                       // Shared argument pointer
    
    // Current Task type (used at runtime)
    EmirMtmqTask* _current_task;                          // Current Task type to run
    
    // Pre-allocated task allocation results for each mode
    std::vector<std::vector<void*> > _thread_jobs_td; // Pre-allocated TD mode: Job list for each thread
    std::vector<std::vector<void*> > _thread_jobs_bu; // Pre-allocated BU mode: Job list for each thread
    std::vector<std::vector<void*> > _thread_jobs_rd; // Pre-allocated RD mode: Job list for each thread
    std::vector<std::vector<void*> >* _current_thread_jobs; // Pointer to current mode's pre-allocated jobs
    pthread_mutex_t _allocation_mutex;            // Allocation mutex
    ExecutionMode _current_mode;                  // Current execution mode (obtained from Task)
    
    // Thread synchronization (used in TD/BU modes)
    std::vector<pthread_mutex_t> _thread_mutexes;     // Mutex for each thread
    std::vector<pthread_cond_t> _thread_conditions;   // Condition variable for each thread
    std::vector<bool> _thread_ready;                  // Whether each thread is ready to execute
    std::vector<bool> _thread_done;                   // Whether each thread is done
    
    // Main thread synchronization (TD/BU modes)
    bool _top_job_done;                          // Whether top job is done (TD mode)
    bool _leaf_jobs_done;                        // Whether leaf jobs are done (BU mode)
    pthread_mutex_t _sync_mutex;                  // Synchronization mutex
    pthread_cond_t _sync_condition;              // Synchronization condition variable
    
    // Control flags
    bool _stop;                                  // Stop flag
    bool _running;                               // Whether running
    bool _task_ready;                            // Whether task is ready to execute
    bool _started;                               // Whether threads have been started
    int _debug;                                  // Debug level (>=1 for basic debug output)
    
    // Thread argument structure
    struct ThreadArg {
        EmirMtmqMgr* mgr;
        size_t thread_id;
        ThreadArg(EmirMtmqMgr* m, size_t id) : mgr(m), thread_id(id) {}
    };
};


#endif // EMIRMTMQMGR_H

