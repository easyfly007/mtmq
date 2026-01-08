#include "emirMtmqTask.h"

// ============================================================================
// EmirMtmqTask base class implementation
// ============================================================================
EmirMtmqTask::EmirMtmqTask(const std::string& name)
    : _name(name)
{
}

EmirMtmqTask::~EmirMtmqTask() {
}

const std::string& EmirMtmqTask::getName() const {
    return _name;
}

void EmirMtmqTask::setName(const std::string& name) {
    _name = name;
}

// ============================================================================
// EmirMtmqRDtask implementation
// ============================================================================
EmirMtmqRDtask::EmirMtmqRDtask(const std::string& name)
    : EmirMtmqTask(name)
{
}

EmirMtmqRDtask::~EmirMtmqRDtask() {
}

ExecutionMode EmirMtmqRDtask::getMode() const {
    return RD;
}

// ============================================================================
// EmirMtmqTDtask implementation
// ============================================================================
EmirMtmqTDtask::EmirMtmqTDtask(const std::string& name)
    : EmirMtmqTask(name)
{
}

EmirMtmqTDtask::~EmirMtmqTDtask() {
}

ExecutionMode EmirMtmqTDtask::getMode() const {
    return TD;
}

// ============================================================================
// EmirMtmqBUtask implementation
// ============================================================================
EmirMtmqBUtask::EmirMtmqBUtask(const std::string& name)
    : EmirMtmqTask(name)
{
}

EmirMtmqBUtask::~EmirMtmqBUtask() {
}

ExecutionMode EmirMtmqBUtask::getMode() const {
    return BU;
}

