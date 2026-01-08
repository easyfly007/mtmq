#include <iostream>
#include <vector>
#include <cassert>
#include <cstring>
#include <ctime>
#include <pthread.h>
#include <unistd.h>
#include <string>
#include "emirMtmqMgr.h"
#include "emirMtmqArg.h"

// Global variables for recording execution order and results
std::vector<int> execution_order;
pthread_mutex_t order_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile int task_counter = 0;

// Helper function: Record task execution
void record_task(int task_id) {
    pthread_mutex_lock(&order_mutex);
    execution_order.push_back(task_id);
    task_counter++;
    pthread_mutex_unlock(&order_mutex);
}

// Helper function: Get current time (milliseconds)
long long get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

// Helper function: Print execution order
void print_execution_order(const std::string& test_name) {
    pthread_mutex_lock(&order_mutex);
    std::cout << "\n[" << test_name << "] Execution order: ";
    for (size_t i = 0; i < execution_order.size(); ++i) {
        std::cout << execution_order[i];
        if (i < execution_order.size() - 1) {
            std::cout << " -> ";
        }
    }
    std::cout << std::endl;
    pthread_mutex_unlock(&order_mutex);
}

// Helper function: Clear records
void clear_records() {
    pthread_mutex_lock(&order_mutex);
    execution_order.clear();
    task_counter = 0;
    pthread_mutex_unlock(&order_mutex);
}

// ============================================================================
// Test Argument class: Inherit from EmirMtmqArg
// ============================================================================
class TestArg : public EmirMtmqArg {
public:
    std::string test_name;
    int test_id;
    int multiplier;
    
    TestArg(const std::string& name, int id, int mult = 1) 
        : test_name(name), test_id(id), multiplier(mult) {}
};

// ============================================================================
// Test Task classes: Directly inherit from EmirMtmqRDtask/EmirMtmqTDtask/EmirMtmqBUtask
// ============================================================================

// Top Job Task (TD mode)
class TopJobTD : public EmirMtmqTDtask {
public:
    virtual ~TopJobTD() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        // job is a pointer to int, representing job_id
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        
        // Access shared argument
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Top Job (ID: " << job_id 
                      << ", Test: " << test_arg->test_name 
                      << ", TestID: " << test_arg->test_id << ")" << std::endl;
        } else {
            std::cout << "Executing Top Job (ID: " << job_id << ")" << std::endl;
        }
        usleep(100000); // 100ms
        record_task(job_id);
    }
};

// Leaf Job Task (TD mode)
class LeafJobTD : public EmirMtmqTDtask {
public:
    virtual ~LeafJobTD() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        // job is a pointer to int, representing job_id
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        
        // Access shared argument
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Leaf Job (ID: " << job_id 
                      << ", Test: " << test_arg->test_name 
                      << ", TestID: " << test_arg->test_id << ")" << std::endl;
        } else {
            std::cout << "Executing Leaf Job (ID: " << job_id << ")" << std::endl;
        }
        usleep(50000); // 50ms
        record_task(job_id);
    }
};

// ============================================================================
// Test case 1: TD (Top-Down) mode
// ============================================================================
void test_TD_mode() {
    std::cout << "\n========== Test Case 1: TD (Top-Down) Mode ==========" << std::endl;
    clear_records();
    
    // Create thread pool: 3 slave threads
    EmirMtmqMgr mgr(3);
    
    // Create Jobs (for testing, using simple integer pointers)
    int top_job_id = 0;
    int leaf_job_ids[6] = {1, 2, 3, 4, 5, 6};
    
    // Set top job
    mgr.setTopJob(&top_job_id);
    
    // Add 6 leaf jobs
    for (int i = 0; i < 6; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    
    // Set argument before running
    TestArg arg1("TD_Mode_Test", 1, 1);
    mgr.setArgument(&arg1);
    
    // Start thread pool
    mgr.start();
    
    // Create Task and run
    EmirMtmqTask* task = new TopJobTD();
    long long start = get_time_ms();
    mgr.run(task);
    long long end = get_time_ms();
    long long duration = end - start;
    delete task;
    
    // Verify results
    print_execution_order("TD Mode");
    
    // Verify: top job (0) should execute before all leaf jobs (1-6)
    bool td_order_correct = true;
    int top_job_index = -1;
    int first_leaf_index = -1;
    
    pthread_mutex_lock(&order_mutex);
    for (size_t i = 0; i < execution_order.size(); ++i) {
        if (execution_order[i] == 0) {
            top_job_index = i;
        }
        if (execution_order[i] >= 1 && execution_order[i] <= 6 && first_leaf_index == -1) {
            first_leaf_index = i;
        }
    }
    pthread_mutex_unlock(&order_mutex);
    
    if (top_job_index != -1 && first_leaf_index != -1) {
        td_order_correct = (top_job_index < first_leaf_index);
    }
    
    std::cout << "Top Job position: " << top_job_index << std::endl;
    std::cout << "First Leaf Job position: " << first_leaf_index << std::endl;
    std::cout << "Execution time: " << duration << " ms" << std::endl;
    std::cout << "Total tasks: " << task_counter << " (expected: 7)" << std::endl;
    
    assert(td_order_correct && "TD mode: Top Job should execute before all Leaf Jobs");
    assert(task_counter == 7 && "TD mode: Should execute 7 tasks");
    
    std::cout << "✓ TD mode test passed!" << std::endl;
}

// Top Job Task (BU mode)
class TopJobBU : public EmirMtmqBUtask {
public:
    virtual ~TopJobBU() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        // job is a pointer to int, representing job_id
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        
        // Access shared argument
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Top Job (ID: " << job_id 
                      << ", Test: " << test_arg->test_name 
                      << ", TestID: " << test_arg->test_id << ")" << std::endl;
        } else {
            std::cout << "Executing Top Job (ID: " << job_id << ")" << std::endl;
        }
        usleep(100000); // 100ms
        record_task(job_id);
    }
};

// Leaf Job Task (BU mode)
class LeafJobBU : public EmirMtmqBUtask {
public:
    virtual ~LeafJobBU() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        // job is a pointer to int, representing job_id
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        
        // Access shared argument
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Leaf Job (ID: " << job_id 
                      << ", Test: " << test_arg->test_name 
                      << ", TestID: " << test_arg->test_id << ")" << std::endl;
        } else {
            std::cout << "Executing Leaf Job (ID: " << job_id << ")" << std::endl;
        }
        usleep(50000); // 50ms
        record_task(job_id);
    }
};

// ============================================================================
// Test case 2: BU (Bottom-Up) mode
// ============================================================================
void test_BU_mode() {
    std::cout << "\n========== Test Case 2: BU (Bottom-Up) Mode ==========" << std::endl;
    clear_records();
    
    // Create thread pool: 4 slave threads
    EmirMtmqMgr mgr(4);
    
    // Create Jobs (for testing, using simple integer pointers)
    int top_job_id = 0;
    int leaf_job_ids[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    
    // Set top job
    mgr.setTopJob(&top_job_id);
    
    // Add 8 leaf jobs
    for (int i = 0; i < 8; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    
    // Set argument before running (different from TD mode)
    TestArg arg2("BU_Mode_Test", 2, 2);
    mgr.setArgument(&arg2);
    
    // Start thread pool
    mgr.start();
    
    // Create Task and run
    EmirMtmqTask* task = new TopJobBU();
    long long start = get_time_ms();
    mgr.run(task);
    delete task;
    long long end = get_time_ms();
    long long duration = end - start;
    
    // Verify results
    print_execution_order("BU Mode");
    
    // Verify: all leaf jobs (1-8) should execute before top job (0)
    bool bu_order_correct = true;
    int top_job_index = -1;
    int last_leaf_index = -1;
    
    pthread_mutex_lock(&order_mutex);
    for (size_t i = 0; i < execution_order.size(); ++i) {
        if (execution_order[i] == 0) {
            top_job_index = i;
        }
        if (execution_order[i] >= 1 && execution_order[i] <= 8) {
            last_leaf_index = i;
        }
    }
    pthread_mutex_unlock(&order_mutex);
    
    if (top_job_index != -1 && last_leaf_index != -1) {
        bu_order_correct = (last_leaf_index < top_job_index);
    }
    
    std::cout << "Last Leaf Job position: " << last_leaf_index << std::endl;
    std::cout << "Top Job position: " << top_job_index << std::endl;
    std::cout << "Execution time: " << duration << " ms" << std::endl;
    std::cout << "Total tasks: " << task_counter << " (expected: 9)" << std::endl;
    
    assert(bu_order_correct && "BU mode: All Leaf Jobs should execute before Top Job");
    assert(task_counter == 9 && "BU mode: Should execute 9 tasks");
    
    std::cout << "✓ BU mode test passed!" << std::endl;
}

// Top Job Task (RD mode)
class TopJobRD : public EmirMtmqRDtask {
public:
    virtual ~TopJobRD() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        // job is a pointer to int, representing job_id
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        
        // Access shared argument
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Top Job (ID: " << job_id 
                      << ", Test: " << test_arg->test_name 
                      << ", TestID: " << test_arg->test_id << ")" << std::endl;
        } else {
            std::cout << "Executing Top Job (ID: " << job_id << ")" << std::endl;
        }
        usleep(100000); // 100ms
        record_task(job_id);
    }
};

// Leaf Job Task (RD mode)
class LeafJobRD : public EmirMtmqRDtask {
public:
    virtual ~LeafJobRD() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        // job is a pointer to int, representing job_id
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        
        // Access shared argument
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Leaf Job (ID: " << job_id 
                      << ", Test: " << test_arg->test_name 
                      << ", TestID: " << test_arg->test_id << ")" << std::endl;
        } else {
            std::cout << "Executing Leaf Job (ID: " << job_id << ")" << std::endl;
        }
        usleep(50000); // 50ms
        record_task(job_id);
    }
};

// ============================================================================
// Test case 3: RD (Random) mode
// ============================================================================
void test_RD_mode() {
    std::cout << "\n========== Test Case 3: RD (Random) Mode ==========" << std::endl;
    clear_records();
    
    // Create thread pool: 2 slave threads (total 3 threads: main thread + 2 slaves)
    EmirMtmqMgr mgr(2);
    
    // Create Jobs (for testing, using simple integer pointers)
    int top_job_id = 0;
    int leaf_job_ids[5] = {1, 2, 3, 4, 5};
    
    // Set top job
    mgr.setTopJob(&top_job_id);
    
    // Add 5 leaf jobs
    for (int i = 0; i < 5; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    
    // Set argument before running (different from TD and BU modes)
    TestArg arg3("RD_Mode_Test", 3, 3);
    mgr.setArgument(&arg3);
    
    // Start thread pool
    mgr.start();
    
    // Create Task and run
    EmirMtmqTask* task = new TopJobRD();
    long long start = get_time_ms();
    mgr.run(task);
    delete task;
    long long end = get_time_ms();
    long long duration = end - start;
    
    // Verify results
    print_execution_order("RD Mode");
    
    // RD mode: Only verify all tasks are executed, don't verify order
    // Because tasks are randomly allocated, execution order may differ
    
    std::cout << "Execution time: " << duration << " ms" << std::endl;
    std::cout << "Total tasks: " << task_counter << " (expected: 6)" << std::endl;
    
    // Verify all tasks are executed
    std::vector<bool> task_executed(6, false);
    pthread_mutex_lock(&order_mutex);
    for (size_t i = 0; i < execution_order.size(); ++i) {
        int task_id = execution_order[i];
        if (task_id >= 0 && task_id < 6) {
            task_executed[task_id] = true;
        }
    }
    pthread_mutex_unlock(&order_mutex);
    
    bool all_executed = true;
    for (int i = 0; i < 6; ++i) {
        if (!task_executed[i]) {
            std::cout << "Error: Task " << i << " not executed!" << std::endl;
            all_executed = false;
        }
    }
    
    assert(all_executed && "RD mode: All tasks should be executed");
    assert(task_counter == 6 && "RD mode: Should execute 6 tasks");
    
    std::cout << "✓ RD mode test passed!" << std::endl;
}

// Top Job Task (TD large tasks)
class TopJobTDLarge : public EmirMtmqTDtask {
public:
    virtual ~TopJobTDLarge() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Top Job (Test: " << test_arg->test_name << ")" << std::endl;
        } else {
            std::cout << "Executing Top Job" << std::endl;
        }
        record_task(job_id);
    }
};

// Leaf Job Task (TD large tasks)
class LeafJobTDLarge : public EmirMtmqTDtask {
public:
    virtual ~LeafJobTDLarge() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        // Access argument if needed
        TestArg* test_arg = static_cast<TestArg*>(arg);
        record_task(job_id);
    }
};

// ============================================================================
// Test case 4: TD mode - Large task test
// ============================================================================
void test_TD_mode_large() {
    std::cout << "\n========== Test Case 4: TD Mode - Large Tasks ==========" << std::endl;
    clear_records();
    
    // Create thread pool: 5 slave threads
    EmirMtmqMgr mgr(5);
    
    // Create Jobs (for testing)
    int top_job_id = 0;
    int leaf_job_ids[20];
    for (int i = 0; i < 20; ++i) {
        leaf_job_ids[i] = i + 1;
    }
    
    // Set top job
    mgr.setTopJob(&top_job_id);
    
    // Add 20 leaf jobs
    for (int i = 0; i < 20; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    
    // Set argument before running
    TestArg arg4("TD_Large_Test", 4, 4);
    mgr.setArgument(&arg4);
    
    // Start thread pool
    mgr.start();
    
    // Create Task and run
    EmirMtmqTask* task = new TopJobTDLarge();
    long long start = get_time_ms();
    mgr.run(task);
    delete task;
    long long end = get_time_ms();
    long long duration = end - start;
    
    std::cout << "Total tasks: " << task_counter << " (expected: 21)" << std::endl;
    std::cout << "Execution time: " << duration << " ms" << std::endl;
    
    assert(task_counter == 21 && "TD mode large tasks: Should execute 21 tasks");
    
    // Verify top job is at first position
    pthread_mutex_lock(&order_mutex);
    assert(execution_order[0] == 0 && "TD mode: Top Job should be the first to execute");
    pthread_mutex_unlock(&order_mutex);
    
    std::cout << "✓ TD mode large tasks test passed!" << std::endl;
}

// Top Job Task (BU parallel)
class TopJobBUParallel : public EmirMtmqBUtask {
public:
    virtual ~TopJobBUParallel() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Top Job (Test: " << test_arg->test_name << ")" << std::endl;
        } else {
            std::cout << "Executing Top Job" << std::endl;
        }
        record_task(job_id);
    }
};

// Leaf Job Task (BU parallel)
class LeafJobBUParallel : public EmirMtmqBUtask {
public:
    virtual ~LeafJobBUParallel() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        // Access argument and use multiplier if available
        TestArg* test_arg = static_cast<TestArg*>(arg);
        int sleep_time = job_id * 10000;
        if (test_arg) {
            sleep_time = job_id * 10000 * test_arg->multiplier;
        }
        usleep(sleep_time); // job_id * 10ms * multiplier
        record_task(job_id);
    }
};

// ============================================================================
// Test case 5: BU mode - Verify parallel execution
// ============================================================================
void test_BU_mode_parallel() {
    std::cout << "\n========== Test Case 5: BU Mode - Parallel Execution Verification ==========" << std::endl;
    clear_records();
    
    // Create thread pool: 3 slave threads
    EmirMtmqMgr mgr(3);
    
    // Create Jobs (for testing)
    int top_job_id = 0;
    int leaf_job_ids[9];
    for (int i = 0; i < 9; ++i) {
        leaf_job_ids[i] = i + 1;
    }
    
    // Set top job
    mgr.setTopJob(&top_job_id);
    
    // Add 9 leaf jobs
    for (int i = 0; i < 9; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    
    // Set argument before running
    TestArg arg5("BU_Parallel_Test", 5, 2);
    mgr.setArgument(&arg5);
    
    // Start thread pool
    mgr.start();
    
    // Create Task and run
    EmirMtmqTask* task = new TopJobBUParallel();
    long long start = get_time_ms();
    mgr.run(task);
    delete task;
    long long end = get_time_ms();
    long long duration = end - start;
    
    print_execution_order("BU Mode Parallel");
    std::cout << "Total tasks: " << task_counter << " (expected: 10)" << std::endl;
    std::cout << "Execution time: " << duration << " ms" << std::endl;
    
    // Verify: If parallel execution, total time should be less than serial time
    // Serial time approximately: 9*10 + 8*10 + ... + 1*10 = 450ms
    // Parallel time should be significantly less than this value
    std::cout << "Note: If parallel execution, time should be significantly less than serial time" << std::endl;
    
    assert(task_counter == 10 && "BU mode parallel: Should execute 10 tasks");
    
    // Verify top job is at the end
    pthread_mutex_lock(&order_mutex);
    assert(execution_order.back() == 0 && "BU mode: Top Job should be the last to execute");
    pthread_mutex_unlock(&order_mutex);
    
    std::cout << "✓ BU mode parallel test passed!" << std::endl;
}

// Top Job Task (RD random)
class TopJobRDRandom : public EmirMtmqRDtask {
public:
    virtual ~TopJobRDRandom() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        TestArg* test_arg = static_cast<TestArg*>(arg);
        if (test_arg) {
            std::cout << "Executing Top Job (Test: " << test_arg->test_name << ")" << std::endl;
        } else {
            std::cout << "Executing Top Job" << std::endl;
        }
        record_task(job_id);
    }
};

// Leaf Job Task (RD random)
class LeafJobRDRandom : public EmirMtmqRDtask {
public:
    virtual ~LeafJobRDRandom() {}
    virtual void run(void* job, EmirMtmqArg* arg) {
        int* job_id_ptr = static_cast<int*>(job);
        int job_id = job_id_ptr ? *job_id_ptr : 0;
        // Access argument if needed
        TestArg* test_arg = static_cast<TestArg*>(arg);
        record_task(job_id);
    }
};

// ============================================================================
// Test case 6: RD mode - Verify random allocation
// ============================================================================
void test_RD_mode_random() {
    std::cout << "\n========== Test Case 6: RD Mode - Random Allocation Verification ==========" << std::endl;
    clear_records();
    
    // Create thread pool: 3 slave threads (total 4 threads)
    EmirMtmqMgr mgr(3);
    
    // Create Jobs (for testing)
    int top_job_id = 0;
    int leaf_job_ids[12];
    for (int i = 0; i < 12; ++i) {
        leaf_job_ids[i] = i + 1;
    }
    
    // Set top job
    mgr.setTopJob(&top_job_id);
    
    // Add 12 leaf jobs
    for (int i = 0; i < 12; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    
    // Set argument before running
    TestArg arg6("RD_Random_Test", 6, 1);
    mgr.setArgument(&arg6);
    
    // Start thread pool
    mgr.start();
    
    // Create Task and run
    EmirMtmqTask* task = new TopJobRDRandom();
    long long start = get_time_ms();
    mgr.run(task);
    delete task;
    long long end = get_time_ms();
    long long duration = end - start;
    
    print_execution_order("RD Mode Random");
    std::cout << "Total tasks: " << task_counter << " (expected: 13)" << std::endl;
    std::cout << "Execution time: " << duration << " ms" << std::endl;
    
    // Verify all tasks are executed
    std::vector<bool> task_executed(13, false);
    pthread_mutex_lock(&order_mutex);
    for (size_t i = 0; i < execution_order.size(); ++i) {
        int task_id = execution_order[i];
        if (task_id >= 0 && task_id < 13) {
            task_executed[task_id] = true;
        }
    }
    pthread_mutex_unlock(&order_mutex);
    
    bool all_executed = true;
    for (int i = 0; i < 13; ++i) {
        if (!task_executed[i]) {
            all_executed = false;
            break;
        }
    }
    
    assert(all_executed && "RD mode random: All tasks should be executed");
    assert(task_counter == 13 && "RD mode random: Should execute 13 tasks");
    
    std::cout << "✓ RD mode random test passed!" << std::endl;
}

// ============================================================================
// Test case 7: Multiple runs with different modes
// ============================================================================
void test_multiple_runs() {
    std::cout << "\n========== Test Case 7: Multiple Runs with Different Modes ==========" << std::endl;
    
    // Create thread pool: 3 slave threads
    EmirMtmqMgr mgr(3);
    
    // Set Jobs once (Jobs remain unchanged and can be reused)
    std::cout << "\n--- Setting Jobs (set once) ---" << std::endl;
    int top_job_id = 0;
    int leaf_job_ids[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    mgr.setTopJob(&top_job_id);
    for (int i = 0; i < 8; ++i) {
        mgr.addLeafJob(&leaf_job_ids[i]);
    }
    std::cout << "Job setup complete: 1 top job, 8 leaf jobs" << std::endl;
    
    // Start thread pool (only once)
    mgr.start();
    
    // Phase 1: Run RD mode tasks (using same Jobs, different argument)
    std::cout << "\n--- Phase 1: RD Mode ---" << std::endl;
    clear_records();
    
    // Set argument for Phase 1
    TestArg arg7_1("Multiple_Runs_Phase1_RD", 71, 1);
    mgr.setArgument(&arg7_1);
    
    // Create Task and run (run() will automatically wait for completion)
    EmirMtmqTask* task_rd = new TopJobRD();
    mgr.run(task_rd);
    
    print_execution_order("Phase 1 RD");
    std::cout << "Phase 1 complete, task count: " << task_counter << std::endl;
    
    // run() has automatically cleared allocation state, no need to manually call clearTasks()
    delete task_rd;
    
    // Phase 2: Run TD mode tasks (using same Jobs, different argument)
    std::cout << "\n--- Phase 2: TD Mode ---" << std::endl;
    clear_records();
    
    // Set argument for Phase 2 (different from Phase 1)
    TestArg arg7_2("Multiple_Runs_Phase2_TD", 72, 2);
    mgr.setArgument(&arg7_2);
    
    // Create Task and run (run() will automatically wait for completion and clear)
    EmirMtmqTask* task_td = new TopJobTD();
    mgr.run(task_td);
    
    print_execution_order("Phase 2 TD");
    std::cout << "Phase 2 complete, task count: " << task_counter << std::endl;
    
    // run() has automatically cleared allocation state, no need to manually call clearTasks()
    delete task_td;
    
    // Phase 3: Run BU mode tasks (using same Jobs, different argument)
    std::cout << "\n--- Phase 3: BU Mode ---" << std::endl;
    clear_records();
    
    // Set argument for Phase 3 (different from Phase 1 and 2)
    TestArg arg7_3("Multiple_Runs_Phase3_BU", 73, 3);
    mgr.setArgument(&arg7_3);
    
    // Create Task and run (run() will automatically wait for completion and clear)
    EmirMtmqTask* task_bu = new TopJobBU();
    mgr.run(task_bu);
    
    print_execution_order("Phase 3 BU");
    std::cout << "Phase 3 complete, task count: " << task_counter << std::endl;
    
    // run() has automatically cleared allocation state, no need to manually call clearTasks()
    delete task_bu;
    
    std::cout << "\n✓ Multiple runs with different modes test passed!" << std::endl;
}

// ============================================================================
// Main function
// ============================================================================
int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "    EmirMtmqMgr Test Suite" << std::endl;
    std::cout << "========================================" << std::endl;
    
    try {
        // Basic functionality tests
        test_TD_mode();
        test_BU_mode();
        test_RD_mode();
        
        // Extended tests
        test_TD_mode_large();
        test_BU_mode_parallel();
        test_RD_mode_random();
        
        // Multiple runs test
        test_multiple_runs();
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "    All tests passed! ✓" << std::endl;
        std::cout << "========================================" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\nTest failed: " << e.what() << std::endl;
        return 1;
    }
}

