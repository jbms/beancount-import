import concurrent.futures
import threading

class DaemonThreadExecutor(concurrent.futures.Executor):
    """Launches each task in a separate daemon thread."""

    def submit(self, fn, *args, **kwargs):
        f = concurrent.futures.Future()
        def wrapper():
            if not f.set_running_or_notify_cancel():
                return
            try:
                f.set_result(fn(*args, **kwargs))
            except Exception as e:
                f.set_exception(e)

        t = threading.Thread(target = wrapper)
        t.daemon = True
        t.start()
        return f


def call_in_new_thread(f, *args, **kwargs):
    executor = DaemonThreadExecutor()
    return executor.submit(f, *args, **kwargs)
