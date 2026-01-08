#include "emirMtmqDebug.h"

// ============================================================================
// Static member initialization
// ============================================================================
pthread_mutex_t EmirMtmqDebug::_output_mutex = PTHREAD_MUTEX_INITIALIZER;
bool EmirMtmqDebug::_mutex_initialized = false;
pthread_key_t EmirMtmqDebug::_buffer_key;
bool EmirMtmqDebug::_key_initialized = false;

// ============================================================================
// Constructor
// ============================================================================
EmirMtmqDebug::EmirMtmqDebug() {
    initMutex();
    initKey();
}

// ============================================================================
// Destructor
// ============================================================================
EmirMtmqDebug::~EmirMtmqDebug() {
    // Mutex is static and shared, don't destroy here
    // It will be destroyed when program exits
}

// ============================================================================
// Initialize mutex (thread-safe initialization)
// ============================================================================
void EmirMtmqDebug::initMutex() {
    // In C++98/03, static initialization with PTHREAD_MUTEX_INITIALIZER
    // should be sufficient, but we can add a check for safety
    if (!_mutex_initialized) {
        // Mutex is already initialized by PTHREAD_MUTEX_INITIALIZER
        // Just mark as initialized
        _mutex_initialized = true;
    }
}

// ============================================================================
// Initialize thread-local storage key
// ============================================================================
void EmirMtmqDebug::initKey() {
    if (!_key_initialized) {
        pthread_key_create(&_buffer_key, cleanupBuffer);
        _key_initialized = true;
    }
}

// ============================================================================
// Destroy mutex
// ============================================================================
void EmirMtmqDebug::destroyMutex() {
    if (_mutex_initialized) {
        pthread_mutex_destroy(&_output_mutex);
        _mutex_initialized = false;
    }
}

// ============================================================================
// Destroy thread-local storage key
// ============================================================================
void EmirMtmqDebug::destroyKey() {
    if (_key_initialized) {
        pthread_key_delete(_buffer_key);
        _key_initialized = false;
    }
}

// ============================================================================
// Cleanup function for thread-local buffer (called when thread exits)
// ============================================================================
void EmirMtmqDebug::cleanupBuffer(void* ptr) {
    if (ptr) {
        std::ostringstream* buffer = static_cast<std::ostringstream*>(ptr);
        delete buffer;
    }
}

// ============================================================================
// Get thread-local buffer (create if not exists)
// ============================================================================
std::ostringstream* EmirMtmqDebug::getThreadBuffer() {
    std::ostringstream* buffer = static_cast<std::ostringstream*>(pthread_getspecific(_buffer_key));
    if (buffer == NULL) {
        // Create new buffer for this thread
        buffer = new std::ostringstream();
        pthread_setspecific(_buffer_key, buffer);
    }
    return buffer;
}

// ============================================================================
// Flush buffer to std::cout (thread-safe)
// ============================================================================
void EmirMtmqDebug::flushBuffer() {
    std::ostringstream* buffer = getThreadBuffer();
    if (buffer && buffer->str().length() > 0) {
        // Lock mutex before output
        pthread_mutex_lock(&_output_mutex);
        
        // Output entire buffer content
        std::cout << buffer->str();
        std::cout.flush();
        
        // Unlock mutex
        pthread_mutex_unlock(&_output_mutex);
        
        // Clear buffer for next use
        buffer->str("");
        buffer->clear();
    }
}

// ============================================================================
// Global instance
// ============================================================================
EmirMtmqDebug emir_debug;

