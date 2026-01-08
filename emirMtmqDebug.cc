#include "emirMtmqDebug.h"

// ============================================================================
// Static member initialization
// ============================================================================
pthread_mutex_t EmirMtmqDebug::_output_mutex = PTHREAD_MUTEX_INITIALIZER;
bool EmirMtmqDebug::_mutex_initialized = false;

// ============================================================================
// Constructor
// ============================================================================
EmirMtmqDebug::EmirMtmqDebug() {
    initMutex();
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
// Destroy mutex
// ============================================================================
void EmirMtmqDebug::destroyMutex() {
    if (_mutex_initialized) {
        pthread_mutex_destroy(&_output_mutex);
        _mutex_initialized = false;
    }
}

// ============================================================================
// Global instance
// ============================================================================
EmirMtmqDebug emir_debug;

