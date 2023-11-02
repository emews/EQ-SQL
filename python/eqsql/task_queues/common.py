from enum import IntEnum


class ResultStatus(IntEnum):
    """Enum defining the status (success or failure) of an EQSQL database
    operation.
    """
    SUCCESS = 0
    FAILURE = 1


EQ_ABORT = 'EQ_ABORT'
EQ_TIMEOUT = 'EQ_TIMEOUT'
EQ_STOP = 'EQ_STOP'


class TaskStatus(IntEnum):
    """Enum defining the status of a task: queued, etc. These are used
    in the database to store the status of a task.
    """
    QUEUED = 0
    RUNNING = 1
    COMPLETE = 2
    CANCELED = 3
    REQUEUED = 4


class StopConditionException(Exception):
    def __init__(self, msg='StopIterationException', *args, **kwargs):
        super().__init__(msg, *args, **kwargs)
        pass


class TimeoutError(Exception):
    def __init__(self, msg='TimeoutError', *args, **kwargs):
        """Exception used to indicate that a query has timed out."""
        super().__init__(msg, *args, **kwargs)
        pass
