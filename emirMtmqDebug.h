#ifndef EMIRMTMQDEBUG_H
#define EMIRMTMQDEBUG_H

#include <iostream>
#include <pthread.h>

// ============================================================================
// EmirMtmqDebug - Thread-safe debug output class
// ============================================================================
// This class provides thread-safe output mechanism using mutex to protect
// std::cout operations. It supports chain output similar to std::cout.
//
// Usage:
//   #include "emirMtmqDebug.h"
//   emir_debug << "Thread " << thread_id << " executing job " << job_id << std::endl;
//
// Note: Each << operation locks and unlocks the mutex. For better performance
// when outputting multiple values, consider combining them into a single string
// or using a temporary string stream.
// ============================================================================
class EmirMtmqDebug {
public:
    // Constructor: Initialize mutex if not already initialized
    EmirMtmqDebug();
    
    // Destructor
    ~EmirMtmqDebug();
    
    // Overload << operator for various types
    // Lock mutex before output, unlock after output
    // Each << operation is atomic (locked separately)
    template<typename T>
    EmirMtmqDebug& operator<<(const T& value) {
        pthread_mutex_lock(&_output_mutex);
        std::cout << value;
        std::cout.flush();  // Ensure immediate output
        pthread_mutex_unlock(&_output_mutex);
        return *this;
    }
    
    // Special handling for stream manipulators (endl, flush, etc.)
    // These are function pointers, need special handling
    typedef std::ostream& (*StreamManipulator)(std::ostream&);
    EmirMtmqDebug& operator<<(StreamManipulator manip) {
        pthread_mutex_lock(&_output_mutex);
        manip(std::cout);
        std::cout.flush();
        pthread_mutex_unlock(&_output_mutex);
        return *this;
    }

private:
    static pthread_mutex_t _output_mutex;  // Static mutex shared by all instances
    static bool _mutex_initialized;       // Flag to track mutex initialization
    
    // Initialize mutex (called once)
    static void initMutex();
    
    // Destroy mutex (called once)
    static void destroyMutex();
};

// Global instance for easy use
extern EmirMtmqDebug emir_debug;

#endif // EMIRMTMQDEBUG_H

