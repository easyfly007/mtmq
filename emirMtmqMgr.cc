#include "emirMtmqMgr.h"
#include "emirMtmqDebug.h"
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <iostream>

// ============================================================================
// Constructor
// ============================================================================
EmirMtmqMgr::EmirMtmqMgr(size_t threadCount, int debug)
    : _thread_count(threadCount)
    , _top_job(NULL)
    , _has_top_job(false)
    , _current_task(NULL)
    , _current_thread_jobs(NULL)
    , _current_mode(TD)
    , _top_job_done(false)
    , _leaf_jobs_done(false)
    , _stop(false)
    , _running(false)
    , _task_ready(false)
    , _started(false)
    , _argument(NULL)
    , _debug(debug)
    , _running_job_count(0)
    , _allocation_mode(STATIC_ALLOCATION)
{
    // Initialize mutexes and condition variables
    pthread_mutex_init(&_allocation_mutex, NULL);
    pthread_mutex_init(&_sync_mutex, NULL);
    pthread_cond_init(&_sync_condition, NULL);
    pthread_mutex_init(&_job_pool_mutex, NULL);
    pthread_mutex_init(&_job_count_mutex, NULL);
    pthread_mutex_init(&_job_done_mutex, NULL);
    
    // Initialize thread synchronization structures (+1 is main thread)
    _thread_mutexes.resize(_thread_count + 1);
    _thread_conditions.resize(_thread_count + 1);
    _thread_ready.resize(_thread_count + 1, false);
    _thread_done.resize(_thread_count + 1, false);
    
    for (size_t i = 0; i <= _thread_count; ++i) {
        pthread_mutex_init(&_thread_mutexes[i], NULL);
        pthread_cond_init(&_thread_conditions[i], NULL);
    }
    
    // Threads will be created in start() function
}

// ============================================================================
// Destructor
// ============================================================================
EmirMtmqMgr::~EmirMtmqMgr() {
    // Only shutdown and join threads if they have been started
    if (_started) {
        shutdown();
        
        // Wait for all threads to finish
        for (size_t i = 0; i < _workers.size(); ++i) {
            pthread_join(_workers[i], NULL);
        }
    }
    
    // Clean up mutexes and condition variables
    pthread_mutex_destroy(&_allocation_mutex);
    pthread_mutex_destroy(&_sync_mutex);
    pthread_cond_destroy(&_sync_condition);
    pthread_mutex_destroy(&_job_pool_mutex);
    pthread_mutex_destroy(&_job_count_mutex);
    pthread_mutex_destroy(&_job_done_mutex);
    
    for (size_t i = 0; i < _thread_mutexes.size(); ++i) {
        pthread_mutex_destroy(&_thread_mutexes[i]);
        pthread_cond_destroy(&_thread_conditions[i]);
    }
    
    // Job is managed by user, EmirMtmqMgr is not responsible for deletion
    // Here we only clear pointers, not delete Job objects
}

// ============================================================================
// Disable copy and assignment
// ============================================================================
EmirMtmqMgr::EmirMtmqMgr(const EmirMtmqMgr&) {
    // Disable copy
}

EmirMtmqMgr& EmirMtmqMgr::operator=(const EmirMtmqMgr&) {
    // Disable assignment
    return *this;
}

// ============================================================================
// Start thread pool
// ============================================================================
void EmirMtmqMgr::start() {
    // Check if already started
    if (_started) {
        std::cerr << "Warning: Threads have already been started!" << std::endl;
        return;
    }
    
    // Check if there are Jobs
    if (!_has_top_job && _leaf_jobs.empty()) {
        std::cerr << "Error: No jobs set! Please set top job and/or leaf jobs before starting." << std::endl;
        return;
    }
    
    // Pre-allocate all three modes (TD, BU, RD) since all jobs are now set
    allocateAll();
    
    // Create thread pool
    _workers.resize(_thread_count);
    for (size_t i = 0; i < _thread_count; ++i) {
        ThreadArg* arg = new ThreadArg(this, i + 1); // thread_id starts from 1 (0 is main thread)
        if (pthread_create(&_workers[i], NULL, worker_thread, arg) != 0) {
            // Thread creation failed
            std::cerr << "Failed to create thread " << i << std::endl;
        }
    }
    
    _started = true;
    
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Thread pool started: " << _thread_count 
                   << " worker threads, " << (_has_top_job ? 1 : 0) << " top job, " 
                   << _leaf_jobs.size() << " leaf jobs" << std::endl;
    }
}

// ============================================================================
// Worker thread static function
// ============================================================================
void* EmirMtmqMgr::worker_thread(void* arg) {
    ThreadArg* targ = static_cast<ThreadArg*>(arg);
    targ->mgr->worker(targ->thread_id);
    delete targ;
    return NULL;
}

// ============================================================================
// Worker thread function
// ============================================================================
void EmirMtmqMgr::worker(size_t thread_id) {
    while (!_stop) {
        pthread_mutex_lock(&_thread_mutexes[thread_id]);
        
        // Wait for task to be ready to execute
        while (!_thread_ready[thread_id] && !_stop) {
            pthread_cond_wait(&_thread_conditions[thread_id], &_thread_mutexes[thread_id]);
        }
        
        if (_stop) {
            pthread_mutex_unlock(&_thread_mutexes[thread_id]);
            return;
        }
        
        pthread_mutex_unlock(&_thread_mutexes[thread_id]);
        
        // Debug output: worker thread started
        if (_debug >= 1 && _current_task) {
            emir_debug << "[Worker Thread " << thread_id << "]";
            if (!_current_task->getName().empty()) {
                emir_debug << " task=" << _current_task->getName();
            }
            emir_debug << " started execution" << std::endl;
        }
        
        // Check allocation mode
        if (_allocation_mode == DYNAMIC_ALLOCATION) {
            // Dynamic allocation mode: get jobs from shared pool
            while (true) {
                void* job = NULL;
                
                // Try to get a job from the pool
                pthread_mutex_lock(&_job_pool_mutex);
                if (!_job_pool.empty()) {
                    job = _job_pool.front();
                    _job_pool.pop();
                }
                pthread_mutex_unlock(&_job_pool_mutex);
                
                if (job == NULL) {
                    // No more jobs in pool, exit
                    break;
                }
                
                // Debug output: got job from pool
                if (_debug >= 1 && _current_task) {
                    emir_debug << "[Worker Thread " << thread_id << "]";
                    if (!_current_task->getName().empty()) {
                        emir_debug << " task=" << _current_task->getName();
                    }
                    emir_debug << " got job from pool" << std::endl;
                }
                
                // Increment running job count
                pthread_mutex_lock(&_job_count_mutex);
                _running_job_count++;
                pthread_mutex_unlock(&_job_count_mutex);
                
                // Execute the job
                if (_current_task) {
                    _current_task->run(job, _argument);
                }
                
                // Debug output: job completed
                if (_debug >= 1 && _current_task) {
                    emir_debug << "[Worker Thread " << thread_id << "]";
                    if (!_current_task->getName().empty()) {
                        emir_debug << " task=" << _current_task->getName();
                    }
                    emir_debug << " completed job" << std::endl;
                }
                
                // Decrement running job count
                pthread_mutex_lock(&_job_count_mutex);
                _running_job_count--;
                pthread_mutex_unlock(&_job_count_mutex);
            }
        } else if (_allocation_mode == STEAL_ALLOCATION) {
            // Steal allocation mode: execute pre-assigned jobs, then steal from others
            // Get execution mode to check if we should skip top job
            ExecutionMode mode = _current_mode;
            
            // 1. Execute own assigned jobs (check if done before executing)
            if (_current_thread_jobs && thread_id < _current_thread_jobs->size() && _current_task) {
                for (size_t i = 0; i < (*_current_thread_jobs)[thread_id].size(); ++i) {
                    void* job = (*_current_thread_jobs)[thread_id][i];
                    if (job == NULL) continue;
                    
                    // In TD/BU mode, slave threads should not execute top job
                    if ((mode == TD || mode == BU) && job == _top_job) {
                        continue;
                    }
                    
                    // Check if job is already done (stolen and completed by another thread)
                    pthread_mutex_lock(&_job_done_mutex);
                    bool is_done = (_job_done_set.find(job) != _job_done_set.end());
                    pthread_mutex_unlock(&_job_done_mutex);
                    
                    if (is_done) {
                        // Job already done by another thread, skip
                        continue;
                    }
                    
                    // Debug output: executing own assigned job
                    if (_debug >= 1 && _current_task) {
                        emir_debug << "[Worker Thread " << thread_id << "]";
                        if (!_current_task->getName().empty()) {
                            emir_debug << " task=" << _current_task->getName();
                        }
                        emir_debug << " executing own assigned job" << std::endl;
                    }
                    
                    // Execute the job
                    _current_task->run(job, _argument);
                    
                    // Debug output: own assigned job completed
                    if (_debug >= 1 && _current_task) {
                        emir_debug << "[Worker Thread " << thread_id << "]";
                        if (!_current_task->getName().empty()) {
                            emir_debug << " task=" << _current_task->getName();
                        }
                        emir_debug << " completed own assigned job" << std::endl;
                    }
                    
                    // Mark job as done
                    pthread_mutex_lock(&_job_done_mutex);
                    _job_done_set.insert(job);
                    pthread_mutex_unlock(&_job_done_mutex);
                }
            }
            
            // 2. Steal jobs from other threads (only leaf jobs in TD/BU mode)
            while (true) {
                void* stolen_job = NULL;
                
                // Try to steal a job from other threads
                pthread_mutex_lock(&_allocation_mutex);
                if (_current_thread_jobs) {
                    for (size_t t = 0; t < _current_thread_jobs->size(); ++t) {
                        if (t == thread_id) continue;  // Skip own thread
                        
                        for (size_t j = 0; j < (*_current_thread_jobs)[t].size(); ++j) {
                            void* job = (*_current_thread_jobs)[t][j];
                            if (job == NULL) continue;
                            
                            // In TD/BU mode, don't steal top job
                            if ((mode == TD || mode == BU) && job == _top_job) {
                                continue;
                            }
                            
                            // Check if job is already done
                            pthread_mutex_lock(&_job_done_mutex);
                            bool is_done = (_job_done_set.find(job) != _job_done_set.end());
                            if (!is_done) {
                                // Found an undone job, mark as done and steal it
                                _job_done_set.insert(job);
                                stolen_job = job;
                            }
                            pthread_mutex_unlock(&_job_done_mutex);
                            
                            if (stolen_job != NULL) {
                                break;  // Found a job to steal
                            }
                        }
                        
                        if (stolen_job != NULL) {
                            break;  // Found a job to steal
                        }
                    }
                }
                pthread_mutex_unlock(&_allocation_mutex);
                
                if (stolen_job == NULL) {
                    // No more jobs to steal, exit
                    break;
                }
                
                // Debug output: stole a job
                if (_debug >= 1 && _current_task) {
                    emir_debug << "[Worker Thread " << thread_id << "]";
                    if (!_current_task->getName().empty()) {
                        emir_debug << " task=" << _current_task->getName();
                    }
                    emir_debug << " stole a job from another thread" << std::endl;
                }
                
                // Execute the stolen job
                if (_current_task) {
                    _current_task->run(stolen_job, _argument);
                }
                
                // Debug output: stolen job completed
                if (_debug >= 1 && _current_task) {
                    emir_debug << "[Worker Thread " << thread_id << "]";
                    if (!_current_task->getName().empty()) {
                        emir_debug << " task=" << _current_task->getName();
                    }
                    emir_debug << " completed stolen job" << std::endl;
                }
                // Job is already marked as done when we stole it
            }
        } else {
            // Static allocation mode: execute pre-assigned jobs
            if (_current_thread_jobs && thread_id < _current_thread_jobs->size() && _current_task) {
                for (size_t i = 0; i < (*_current_thread_jobs)[thread_id].size(); ++i) {
                    if ((*_current_thread_jobs)[thread_id][i]) {
                        // Debug output: executing assigned job
                        if (_debug >= 1) {
                            emir_debug << "[Worker Thread " << thread_id << "]";
                            if (!_current_task->getName().empty()) {
                                emir_debug << " task=" << _current_task->getName();
                            }
                            emir_debug << " executing assigned job " << (i + 1) << "/" << (*_current_thread_jobs)[thread_id].size() << std::endl;
                        }
                        
                        _current_task->run((*_current_thread_jobs)[thread_id][i], _argument);  // Call EmirMtmqTask's run(void*, EmirMtmqArg*)
                        
                        // Debug output: assigned job completed
                        if (_debug >= 1) {
                            emir_debug << "[Worker Thread " << thread_id << "]";
                            if (!_current_task->getName().empty()) {
                                emir_debug << " task=" << _current_task->getName();
                            }
                            emir_debug << " completed assigned job " << (i + 1) << "/" << (*_current_thread_jobs)[thread_id].size() << std::endl;
                        }
                    }
                }
            }
        }
        
        // Debug output: worker thread completed all jobs
        if (_debug >= 1 && _current_task) {
            emir_debug << "[Worker Thread " << thread_id << "]";
            if (!_current_task->getName().empty()) {
                emir_debug << " task=" << _current_task->getName();
            }
            emir_debug << " completed all jobs" << std::endl;
        }
        
        // Mark as done
        pthread_mutex_lock(&_thread_mutexes[thread_id]);
        _thread_done[thread_id] = true;
        _thread_ready[thread_id] = false;  // Reset, ready to wait for next task
        pthread_mutex_unlock(&_thread_mutexes[thread_id]);
    }
}

// ============================================================================
// TD mode allocation
// ============================================================================
void EmirMtmqMgr::allocateTD() {
    // 1. Initialize thread Job list (vector<void*>)
    _thread_jobs_td.resize(_thread_count + 1);  // +1 is main thread
    for (size_t i = 0; i < _thread_jobs_td.size(); ++i) {
        _thread_jobs_td[i].clear();
    }
    
    // 2. Main thread allocates top job
    if (_has_top_job && _top_job) {
        _thread_jobs_td[0].push_back(_top_job);  // Index 0 is main thread
    }
    
    // 3. Slave threads evenly distribute leaf jobs
    size_t leaf_count = _leaf_jobs.size();
    if (leaf_count > 0 && _thread_count > 0) {
        size_t jobs_per_thread = leaf_count / _thread_count;
        size_t remainder = leaf_count % _thread_count;
        
        size_t job_index = 0;
        for (size_t i = 0; i < _thread_count; ++i) {
            size_t count = jobs_per_thread + (i < remainder ? 1 : 0);
            for (size_t j = 0; j < count; ++j) {
                _thread_jobs_td[i + 1].push_back(_leaf_jobs[job_index++]); // i+1 is slave thread
            }
        }
    }
}

// ============================================================================
// BU mode allocation
// ============================================================================
void EmirMtmqMgr::allocateBU() {
    // 1. Initialize thread Job list
    _thread_jobs_bu.resize(_thread_count + 1);
    for (size_t i = 0; i < _thread_jobs_bu.size(); ++i) {
        _thread_jobs_bu[i].clear();
    }
    
    // 2. Slave threads evenly distribute leaf jobs
    size_t leaf_count = _leaf_jobs.size();
    if (leaf_count > 0 && _thread_count > 0) {
        size_t jobs_per_thread = leaf_count / _thread_count;
        size_t remainder = leaf_count % _thread_count;
        
        size_t job_index = 0;
        for (size_t i = 0; i < _thread_count; ++i) {
            size_t count = jobs_per_thread + (i < remainder ? 1 : 0);
            for (size_t j = 0; j < count; ++j) {
                _thread_jobs_bu[i + 1].push_back(_leaf_jobs[job_index++]);
            }
        }
    }
    
    // 3. Main thread allocates top job (but waits for execution)
    if (_has_top_job && _top_job) {
        _thread_jobs_bu[0].push_back(_top_job);
    }
}

// ============================================================================
// RD mode allocation
// ============================================================================
void EmirMtmqMgr::allocateRD() {
    // 1. Collect all Jobs (using void*)
    std::vector<void*> all_jobs;
    if (_has_top_job && _top_job) {
        all_jobs.push_back(_top_job);
    }
    for (size_t i = 0; i < _leaf_jobs.size(); ++i) {
        all_jobs.push_back(_leaf_jobs[i]);
    }
    
    size_t total_jobs = all_jobs.size();
    size_t total_threads = _thread_count + 1;  // Including main thread
    
    // 2. Initialize thread Job list
    _thread_jobs_rd.resize(total_threads);
    for (size_t i = 0; i < _thread_jobs_rd.size(); ++i) {
        _thread_jobs_rd[i].clear();
    }
    
    // 3. Evenly distribute all Jobs to all threads
    if (total_jobs > 0 && total_threads > 0) {
        size_t jobs_per_thread = total_jobs / total_threads;
        size_t remainder = total_jobs % total_threads;
        
        size_t job_index = 0;
        for (size_t i = 0; i < total_threads; ++i) {
            size_t count = jobs_per_thread + (i < remainder ? 1 : 0);
            for (size_t j = 0; j < count; ++j) {
                _thread_jobs_rd[i].push_back(all_jobs[job_index++]);
            }
        }
    }
}

// ============================================================================
// Clear task allocation state
// ============================================================================
void EmirMtmqMgr::clearAllocation() {
    pthread_mutex_lock(&_allocation_mutex);
    
    // Reset thread state (pre-allocated jobs remain unchanged)
    for (size_t i = 0; i < _thread_ready.size(); ++i) {
        _thread_ready[i] = false;
        _thread_done[i] = false;
    }
    
    _top_job_done = false;
    _leaf_jobs_done = false;
    _current_thread_jobs = NULL;
    
    pthread_mutex_unlock(&_allocation_mutex);
}

// ============================================================================
// Allocate all three modes (called after setTopJob/addLeafJob)
// ============================================================================
void EmirMtmqMgr::allocateAll() {
    pthread_mutex_lock(&_allocation_mutex);
    
    // Allocate all three modes
    allocateTD();
    allocateBU();
    allocateRD();
    
    pthread_mutex_unlock(&_allocation_mutex);
}

// ============================================================================
// Set top job
// ============================================================================
void EmirMtmqMgr::setTopJob(void* job) {
    // Job is managed by user, EmirMtmqMgr only stores pointer
    _top_job = job;
    _has_top_job = true;
    
    // Pre-allocation will be done in start() function after all jobs are set
}

// ============================================================================
// Add leaf job
// ============================================================================
void EmirMtmqMgr::addLeafJob(void* job) {
    // Job is managed by user, EmirMtmqMgr only stores pointer
    _leaf_jobs.push_back(job);
    
    // Pre-allocation will be done in start() function after all jobs are set
}

// ============================================================================
// Set shared argument
// ============================================================================
void EmirMtmqMgr::setArgument(EmirMtmqArg* arg) {
    // Argument is managed by user, EmirMtmqMgr only stores pointer
    _argument = arg;
}

// ============================================================================
// Set allocation mode
// ============================================================================
void EmirMtmqMgr::setAllocationMode(AllocationMode mode) {
    _allocation_mode = mode;
}

// ============================================================================
// Clear task allocation state (does not clear Job, Job remains unchanged)
// ============================================================================
void EmirMtmqMgr::clearTasks() {
    // Wait for current task to complete
    if (_running) {
        // Wait for all threads to complete
        for (size_t i = 0; i < _thread_count; ++i) {
            while (!_thread_done[i + 1]) {
                usleep(1000); // 1ms
            }
        }
    }
    
    // Only clear allocation state, not Job (Job remains unchanged and can be reused)
    clearAllocation();
}

// ============================================================================
// TD mode execution
// ============================================================================
void EmirMtmqMgr::executeTD() {
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Executing TD mode: main thread runs top job first, then slave threads run leaf jobs";
        if (_current_task && !_current_task->getName().empty()) {
            emir_debug << ", task_name=" << _current_task->getName();
        }
        emir_debug << std::endl;
    }
    
    // 1. Initialize synchronization variables
    _top_job_done = false;
    for (size_t i = 0; i < _thread_count; ++i) {
        pthread_mutex_lock(&_thread_mutexes[i + 1]);
        _thread_ready[i + 1] = false;  // Slave threads initially not ready
        _thread_done[i + 1] = false;
        pthread_mutex_unlock(&_thread_mutexes[i + 1]);
    }
    
    // 2. Main thread executes top job
    if (_current_thread_jobs && (*_current_thread_jobs)[0].size() > 0 && _current_task) {
        for (size_t i = 0; i < (*_current_thread_jobs)[0].size(); ++i) {
            if ((*_current_thread_jobs)[0][i]) {
                _current_task->run((*_current_thread_jobs)[0][i], _argument);  // Call EmirMtmqTask's run(void*, EmirMtmqArg*)
            }
        }
    }
    
    // 3. Top job completed, set flag and wake up all slave threads
    _top_job_done = true;
    for (size_t i = 0; i < _thread_count; ++i) {
        pthread_mutex_lock(&_thread_mutexes[i + 1]);
        _thread_ready[i + 1] = true;
        pthread_cond_signal(&_thread_conditions[i + 1]);
        pthread_mutex_unlock(&_thread_mutexes[i + 1]);
    }
    
    // 4. Wait for all slave threads to complete
    for (size_t i = 0; i < _thread_count; ++i) {
        while (!_thread_done[i + 1]) {
            usleep(1000); // 1ms
        }
    }
}

// ============================================================================
// BU mode execution
// ============================================================================
void EmirMtmqMgr::executeBU() {
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Executing BU mode: slave threads run leaf jobs first, then main thread runs top job";
        if (_current_task && !_current_task->getName().empty()) {
            emir_debug << ", task_name=" << _current_task->getName();
        }
        emir_debug << std::endl;
    }
    
    // 1. Initialize synchronization variables
    _leaf_jobs_done = false;
    _thread_ready[0] = false;  // Main thread initially not ready
    
    // 2. Wake up all slave threads to start execution
    for (size_t i = 0; i < _thread_count; ++i) {
        pthread_mutex_lock(&_thread_mutexes[i + 1]);
        _thread_ready[i + 1] = true;
        _thread_done[i + 1] = false;
        pthread_cond_signal(&_thread_conditions[i + 1]);
        pthread_mutex_unlock(&_thread_mutexes[i + 1]);
    }
    
    // 3. Wait for all slave threads to complete
    for (size_t i = 0; i < _thread_count; ++i) {
        while (!_thread_done[i + 1]) {
            usleep(1000); // 1ms
        }
    }
    
    // 4. Leaf jobs completed, main thread executes top job
    _leaf_jobs_done = true;
    if (_current_thread_jobs && (*_current_thread_jobs)[0].size() > 0 && _current_task) {
        for (size_t i = 0; i < (*_current_thread_jobs)[0].size(); ++i) {
            if ((*_current_thread_jobs)[0][i]) {
                _current_task->run((*_current_thread_jobs)[0][i], _argument);  // Call EmirMtmqTask's run(void*, EmirMtmqArg*)
            }
        }
    }
}

// ============================================================================
// RD mode execution
// ============================================================================
void EmirMtmqMgr::executeRD() {
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Executing RD mode: all threads randomly execute all jobs";
        if (_current_task && !_current_task->getName().empty()) {
            emir_debug << ", task_name=" << _current_task->getName();
        }
        emir_debug << std::endl;
    }
    
    // 1. Initialize and wake up all slave threads to start execution
    for (size_t i = 0; i < _thread_count; ++i) {
        pthread_mutex_lock(&_thread_mutexes[i + 1]);
        _thread_ready[i + 1] = true;
        _thread_done[i + 1] = false;
        pthread_cond_signal(&_thread_conditions[i + 1]);
        pthread_mutex_unlock(&_thread_mutexes[i + 1]);
    }
    
    // 2. Main thread executes its own Job
    if (_current_thread_jobs && (*_current_thread_jobs)[0].size() > 0 && _current_task) {
        for (size_t i = 0; i < (*_current_thread_jobs)[0].size(); ++i) {
            if ((*_current_thread_jobs)[0][i]) {
                _current_task->run((*_current_thread_jobs)[0][i], _argument);  // Call EmirMtmqTask's run(void*, EmirMtmqArg*)
            }
        }
    }
    
    // 3. Wait for all slave threads to complete
    for (size_t i = 0; i < _thread_count; ++i) {
        while (!_thread_done[i + 1]) {
            usleep(1000); // 1ms
        }
    }
}

// ============================================================================
// Dynamic allocation mode execution
// ============================================================================
void EmirMtmqMgr::executeDynamic() {
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Executing Dynamic mode: all threads get jobs from shared pool";
        if (_current_task && !_current_task->getName().empty()) {
            emir_debug << ", task_name=" << _current_task->getName();
        }
        emir_debug << std::endl;
    }
    
    // Get execution mode from Task
    ExecutionMode mode = _current_mode;
    
    // 1. Build job pool according to execution mode
    pthread_mutex_lock(&_job_pool_mutex);
    while (!_job_pool.empty()) {
        _job_pool.pop();  // Clear any remaining jobs
    }
    
    // Add jobs to pool based on execution mode
    if (mode == TD) {
        // TD mode: only leaf jobs go to pool, top job will be executed by main thread first
        for (size_t i = 0; i < _leaf_jobs.size(); ++i) {
            _job_pool.push(_leaf_jobs[i]);
        }
    } else if (mode == BU) {
        // BU mode: only leaf jobs go to pool, top job will be executed by main thread after leaf jobs
        for (size_t i = 0; i < _leaf_jobs.size(); ++i) {
            _job_pool.push(_leaf_jobs[i]);
        }
    } else {
        // RD mode: all jobs (top job + leaf jobs) go to pool
        if (_has_top_job && _top_job) {
            _job_pool.push(_top_job);
        }
        for (size_t i = 0; i < _leaf_jobs.size(); ++i) {
            _job_pool.push(_leaf_jobs[i]);
        }
    }
    pthread_mutex_unlock(&_job_pool_mutex);
    
    // 2. Initialize running job count
    pthread_mutex_lock(&_job_count_mutex);
    _running_job_count = 0;
    pthread_mutex_unlock(&_job_count_mutex);
    
    // 3. Handle TD mode: main thread executes top job first
    if (mode == TD && _has_top_job && _top_job && _current_task) {
        _current_task->run(_top_job, _argument);
    }
    
    // 4. Initialize thread synchronization variables and wake up slave threads
    for (size_t i = 0; i < _thread_count; ++i) {
        pthread_mutex_lock(&_thread_mutexes[i + 1]);
        _thread_ready[i + 1] = true;
        _thread_done[i + 1] = false;
        pthread_cond_signal(&_thread_conditions[i + 1]);
        pthread_mutex_unlock(&_thread_mutexes[i + 1]);
    }
    
    // 5. Main thread participates in dynamic allocation (for leaf jobs in TD/BU, or all jobs in RD)
    while (true) {
        void* job = NULL;
        
        // Try to get a job from the pool
        pthread_mutex_lock(&_job_pool_mutex);
        if (!_job_pool.empty()) {
            job = _job_pool.front();
            _job_pool.pop();
        }
        pthread_mutex_unlock(&_job_pool_mutex);
        
        if (job == NULL) {
            // No more jobs in pool, exit
            break;
        }
        
        // Increment running job count
        pthread_mutex_lock(&_job_count_mutex);
        _running_job_count++;
        pthread_mutex_unlock(&_job_count_mutex);
        
        // Execute the job
        if (_current_task) {
            _current_task->run(job, _argument);
        }
        
        // Decrement running job count
        pthread_mutex_lock(&_job_count_mutex);
        _running_job_count--;
        pthread_mutex_unlock(&_job_count_mutex);
    }
    
    // 6. Wait for all slave threads to complete
    for (size_t i = 0; i < _thread_count; ++i) {
        while (!_thread_done[i + 1]) {
            usleep(1000); // 1ms
        }
    }
    
    // 7. Handle BU mode: main thread executes top job after all leaf jobs are done
    if (mode == BU && _has_top_job && _top_job && _current_task) {
        _current_task->run(_top_job, _argument);
    }
    
    // 8. Wait for all running jobs to complete
    while (true) {
        pthread_mutex_lock(&_job_count_mutex);
        int count = _running_job_count;
        pthread_mutex_unlock(&_job_count_mutex);
        
        if (count == 0) {
            break;  // All jobs completed
        }
        
        usleep(1000); // 1ms
    }
}

// ============================================================================
// Steal allocation mode execution
// ============================================================================
void EmirMtmqMgr::executeSteal() {
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Executing Steal mode: start with static allocation, then steal jobs from other threads";
        if (_current_task && !_current_task->getName().empty()) {
            emir_debug << ", task_name=" << _current_task->getName();
        }
        emir_debug << std::endl;
    }
    
    // Get execution mode from Task
    ExecutionMode mode = _current_mode;
    
    // 1. Clear job done set
    pthread_mutex_lock(&_job_done_mutex);
    _job_done_set.clear();
    pthread_mutex_unlock(&_job_done_mutex);
    
    // 2. Handle TD mode: main thread executes top job first
    if (mode == TD && _has_top_job && _top_job && _current_task) {
        // Mark top job as done (so it won't be stolen)
        pthread_mutex_lock(&_job_done_mutex);
        _job_done_set.insert(_top_job);
        pthread_mutex_unlock(&_job_done_mutex);
        
        // Execute top job
        _current_task->run(_top_job, _argument);
    }
    
    // 3. Initialize thread synchronization variables and wake up slave threads
    for (size_t i = 0; i < _thread_count; ++i) {
        pthread_mutex_lock(&_thread_mutexes[i + 1]);
        _thread_ready[i + 1] = true;
        _thread_done[i + 1] = false;
        pthread_cond_signal(&_thread_conditions[i + 1]);
        pthread_mutex_unlock(&_thread_mutexes[i + 1]);
    }
    
    // 4. Main thread participates in steal mode (for leaf jobs in TD/BU, or all jobs in RD)
    // Main thread executes its own assigned jobs (excluding top job in TD/BU mode), then steals from others
    if (_current_thread_jobs && (*_current_thread_jobs)[0].size() > 0 && _current_task) {
        // Execute own assigned jobs (check if done before executing)
        for (size_t i = 0; i < (*_current_thread_jobs)[0].size(); ++i) {
            void* job = (*_current_thread_jobs)[0][i];
            if (job == NULL) continue;
            
            // In TD/BU mode, skip top job (already executed or will be executed later)
            if ((mode == TD || mode == BU) && job == _top_job) {
                continue;
            }
            
            // Check if job is already done (stolen and completed by another thread)
            pthread_mutex_lock(&_job_done_mutex);
            bool is_done = (_job_done_set.find(job) != _job_done_set.end());
            pthread_mutex_unlock(&_job_done_mutex);
            
            if (is_done) {
                // Job already done by another thread, skip
                continue;
            }
            
            // Execute the job
            _current_task->run(job, _argument);
            
            // Mark job as done
            pthread_mutex_lock(&_job_done_mutex);
            _job_done_set.insert(job);
            pthread_mutex_unlock(&_job_done_mutex);
        }
    }
    
    // 5. Main thread steals jobs from other threads (only leaf jobs in TD/BU mode)
    while (true) {
        void* stolen_job = NULL;
        
        // Try to steal a job from other threads
        pthread_mutex_lock(&_allocation_mutex);
        if (_current_thread_jobs) {
            for (size_t t = 0; t < _current_thread_jobs->size(); ++t) {
                if (t == 0) continue;  // Skip main thread (index 0)
                
                for (size_t j = 0; j < (*_current_thread_jobs)[t].size(); ++j) {
                    void* job = (*_current_thread_jobs)[t][j];
                    if (job == NULL) continue;
                    
                    // In TD/BU mode, don't steal top job
                    if ((mode == TD || mode == BU) && job == _top_job) {
                        continue;
                    }
                    
                    // Check if job is already done
                    pthread_mutex_lock(&_job_done_mutex);
                    bool is_done = (_job_done_set.find(job) != _job_done_set.end());
                    if (!is_done) {
                        // Found an undone job, mark as done and steal it
                        _job_done_set.insert(job);
                        stolen_job = job;
                    }
                    pthread_mutex_unlock(&_job_done_mutex);
                    
                    if (stolen_job != NULL) {
                        break;  // Found a job to steal
                    }
                }
                
                if (stolen_job != NULL) {
                    break;  // Found a job to steal
                }
            }
        }
        pthread_mutex_unlock(&_allocation_mutex);
        
        if (stolen_job == NULL) {
            // No more jobs to steal, exit
            break;
        }
        
        // Execute the stolen job
        if (_current_task) {
            _current_task->run(stolen_job, _argument);
        }
        // Job is already marked as done when we stole it
    }
    
    // 6. Wait for all slave threads to complete
    for (size_t i = 0; i < _thread_count; ++i) {
        while (!_thread_done[i + 1]) {
            usleep(1000); // 1ms
        }
    }
    
    // 7. Handle BU mode: main thread executes top job after all leaf jobs are done
    if (mode == BU && _has_top_job && _top_job && _current_task) {
        _current_task->run(_top_job, _argument);
    }
}

// ============================================================================
// Run method
// ============================================================================
void EmirMtmqMgr::run(EmirMtmqTask* task) {
    if (_running) {
        return;  // If already running, wait for completion
    }
    
    // Check if threads have been started
    if (!_started) {
        std::cerr << "Error: Threads have not been started! Please call start() first." << std::endl;
        return;
    }
    
    // Check if there are Jobs
    if (!_has_top_job && _leaf_jobs.empty()) {
        return;  // No Jobs
    }
    
    // Check if Task is set
    if (!task) {
        std::cerr << "Error: Task not set!" << std::endl;
        return;
    }
    
    // Save current Task type
    _current_task = task;
    
    // Get execution mode from Task
    ExecutionMode mode = task->getMode();
    
    // Debug output
    if (_debug >= 1) {
        const char* mode_str = (mode == TD) ? "TD" : (mode == BU) ? "BU" : "RD";
        const char* alloc_str = (_allocation_mode == STATIC_ALLOCATION) ? "STATIC" : 
                                (_allocation_mode == DYNAMIC_ALLOCATION) ? "DYNAMIC" : "STEAL";
        emir_debug << "[EmirMtmqMgr] Starting task execution: mode=" << mode_str 
                   << ", allocation=" << alloc_str;
        if (!task->getName().empty()) {
            emir_debug << ", task_name=" << task->getName();
        }
        emir_debug << std::endl;
    }
    
    // Clear previous allocation state
    clearAllocation();
    
    // Execute according to allocation mode
    if (_allocation_mode == DYNAMIC_ALLOCATION) {
        // Dynamic allocation mode: use shared job pool
        _running = true;
        
        // Reset completion flags
        for (size_t i = 0; i < _thread_count; ++i) {
            pthread_mutex_lock(&_thread_mutexes[i + 1]);
            _thread_done[i + 1] = false;
            pthread_mutex_unlock(&_thread_mutexes[i + 1]);
        }
        
        // Execute in dynamic mode
        executeDynamic();
        
        // Wait for all threads to complete
        for (size_t i = 0; i < _thread_count; ++i) {
            while (!_thread_done[i + 1]) {
                usleep(1000); // 1ms
            }
        }
    } else if (_allocation_mode == STEAL_ALLOCATION) {
        // Steal allocation mode: start with static allocation, then steal from others
        // Set current mode
        _current_mode = mode;
        
        // Use pre-allocated results according to mode
        pthread_mutex_lock(&_allocation_mutex);
        switch (mode) {
            case TD:
                _current_thread_jobs = &_thread_jobs_td;
                break;
            case BU:
                _current_thread_jobs = &_thread_jobs_bu;
                break;
            case RD:
                _current_thread_jobs = &_thread_jobs_rd;
                break;
        }
        pthread_mutex_unlock(&_allocation_mutex);
        
        _running = true;
        
        // Reset completion flags
        for (size_t i = 0; i < _thread_count; ++i) {
            pthread_mutex_lock(&_thread_mutexes[i + 1]);
            _thread_done[i + 1] = false;
            pthread_mutex_unlock(&_thread_mutexes[i + 1]);
        }
        
        // Execute in steal mode
        executeSteal();
        
        // Wait for all threads to complete
        for (size_t i = 0; i < _thread_count; ++i) {
            while (!_thread_done[i + 1]) {
                usleep(1000); // 1ms
            }
        }
        
    } else {
        // Static allocation mode: use pre-allocated results
        // Set current mode
        _current_mode = mode;
        
        // Use pre-allocated results according to mode
        pthread_mutex_lock(&_allocation_mutex);
        switch (mode) {
            case TD:
                _current_thread_jobs = &_thread_jobs_td;
                break;
            case BU:
                _current_thread_jobs = &_thread_jobs_bu;
                break;
            case RD:
                _current_thread_jobs = &_thread_jobs_rd;
                break;
        }
        pthread_mutex_unlock(&_allocation_mutex);
        
        _running = true;
        
        // Reset completion flags (with mutex protection to avoid race condition)
        // Note: executeTD/BU/RD will also reset these flags, but we reset here
        // to ensure clean state before execution
        for (size_t i = 0; i < _thread_count; ++i) {
            pthread_mutex_lock(&_thread_mutexes[i + 1]);
            _thread_done[i + 1] = false;
            pthread_mutex_unlock(&_thread_mutexes[i + 1]);
        }
        
        // Execute according to mode
        switch (mode) {
            case TD:
                executeTD();
                break;
            case BU:
                executeBU();
                break;
            case RD:
                executeRD();
                break;
        }
        
        // Wait for all threads to complete (ensure all tasks are executed)
        for (size_t i = 0; i < _thread_count; ++i) {
            while (!_thread_done[i + 1]) {
                usleep(1000); // 1ms
            }
        }
    }
    
    _running = false;
    
    // Debug output
    if (_debug >= 1) {
        emir_debug << "[EmirMtmqMgr] Task execution completed" << std::endl;
    }
    
    // Automatically clear allocation state (Job remains unchanged and can be reused)
    clearAllocation();
}

// ============================================================================
// Shutdown method
// ============================================================================
void EmirMtmqMgr::shutdown() {
    _stop = true;
    
    // Wake up all waiting threads
    for (size_t i = 0; i < _thread_conditions.size(); ++i) {
        pthread_mutex_lock(&_thread_mutexes[i]);
        pthread_cond_broadcast(&_thread_conditions[i]);
        pthread_mutex_unlock(&_thread_mutexes[i]);
    }
}

// ============================================================================
// Status query methods
// ============================================================================
size_t EmirMtmqMgr::getThreadCount() const {
    return _thread_count;
}

size_t EmirMtmqMgr::getLeafJobCount() const {
    return _leaf_jobs.size();
}

bool EmirMtmqMgr::hasTopJob() const {
    return _has_top_job;
}

bool EmirMtmqMgr::isRunning() const {
    return _running;
}

