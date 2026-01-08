#ifndef EMIRMTMQDEBUG_H
#define EMIRMTMQDEBUG_H

#include <iostream>
#include <sstream>
#include <string>
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
    // Write to thread-local buffer, flush only when endl or flush is encountered
    template<typename T>
    EmirMtmqDebug& operator<<(const T& value) {
        // Get thread-local buffer
        std::ostringstream* buffer = getThreadBuffer();
        if (buffer) {
            *buffer << value;
        }
        return *this;
    }
    
    // Special handling for stream manipulators (endl, flush, etc.)
    // These are function pointers, need special handling
    typedef std::ostream& (*StreamManipulator)(std::ostream&);
    EmirMtmqDebug& operator<<(StreamManipulator manip) {
        std::ostringstream* buffer = getThreadBuffer();
        if (buffer) {
            // Save buffer content before applying manipulator
            std::string before = buffer->str();
            
            // Apply manipulator to buffer
            manip(*buffer);
            
            // Get buffer content after applying manipulator
            std::string after = buffer->str();
            
            // Check if manipulator is endl or flush by comparing function pointers
            // Also check if buffer content changed (endl adds newline, flush doesn't change content but triggers flush)
            bool is_endl = false;
            bool is_flush = false;
            
            // Try to compare function pointers (may not work reliably in C++98/03 for templates)
            // As fallback, check if endl was applied (buffer content changed and contains newline)
            if (manip == static_cast<StreamManipulator>(std::endl)) {
                is_endl = true;
            } else if (manip == static_cast<StreamManipulator>(std::flush)) {
                is_flush = true;
            } else {
                // Fallback: check if buffer content changed and contains newline (likely endl)
                // Note: This is not 100% reliable but works for most cases
                if (after.length() > before.length()) {
                    // Check if new content ends with newline
                    size_t new_content_start = before.length();
                    std::string new_content = after.substr(new_content_start);
                    if (new_content.find('\n') != std::string::npos) {
                        is_endl = true;
                    }
                }
            }
            
            // Only flush when endl or flush is encountered
            if (is_endl || is_flush) {
                flushBuffer();
            }
            // Other manipulators (like std::hex, std::dec, etc.) don't trigger flush
        }
        return *this;
    }

private:
    static pthread_mutex_t _output_mutex;  // Static mutex shared by all instances
    static bool _mutex_initialized;       // Flag to track mutex initialization
    static pthread_key_t _buffer_key;      // Thread-local storage key for buffer
    static bool _key_initialized;         // Flag to track key initialization
    
    // Get thread-local buffer (create if not exists)
    std::ostringstream* getThreadBuffer();
    
    // Flush buffer to std::cout (thread-safe)
    void flushBuffer();
    
    // Initialize mutex and thread-local storage key (called once)
    static void initMutex();
    static void initKey();
    
    // Destroy mutex and thread-local storage key (called once)
    static void destroyMutex();
    static void destroyKey();
    
    // Cleanup function for thread-local buffer (called when thread exits)
    static void cleanupBuffer(void* ptr);
};

// Global instance for easy use
extern EmirMtmqDebug emir_debug;

#endif // EMIRMTMQDEBUG_H

