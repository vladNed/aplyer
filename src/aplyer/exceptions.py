class TaskNotAwaitable(Exception):
    """Only async tasks are permitted"""

    pass


class InvalidTaskData(Exception):
    """Task serialized data cannot be parsed."""

    pass


class TaskNotRegistered(Exception):
    """If a task is received without being registered in the app"""

    pass


class BackendConnectionError(Exception):
    """Raised if the backend is unreachable"""

    pass
