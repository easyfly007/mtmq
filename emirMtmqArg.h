#ifndef EMIRMTMQARG_H
#define EMIRMTMQARG_H

// ============================================================================
// EmirMtmqArg - Base class for shared arguments
// ============================================================================
// This class is used to store global parameters or pointers that are shared
// across all threads. Users can inherit from this class to define their own
// argument types.
// ============================================================================
class EmirMtmqArg {
public:
    virtual ~EmirMtmqArg() {}
    
    // Users can inherit from EmirMtmqArg and add their own members
    // Example:
    //   class MyArg : public EmirMtmqArg {
    //   public:
    //       int some_parameter;
    //       void* some_pointer;
    //   };
};

#endif // EMIRMTMQARG_H

