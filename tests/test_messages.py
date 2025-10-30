from datetime import datetime

from aplyer.tasks.message import Task


def test_serialize_task():
    task = Task(name="test_func", queue="test_queue")
    msg = task.serialize()
    double_task = Task.deserilize(msg)

    assert double_task == task


def test_serialize_task_with_different_args():
    task = Task(
        name="test_func",
        queue="test_queue",
        args=(
            datetime.now().isoformat(),
            12,
            {"name": "val"},
        ),
    )
    msg = task.serialize()
    double_task = Task.deserilize(msg)

    assert double_task == task
