import unittest
import os


def take_times():
    """Return a tuple of (user, system, elapsed)"""
    times = os.times()
    return times[0], times[1], times[-1]


def format_seconds(secs):
    if secs < 60:
        if secs > 0.1:
            return '%.1fs' % secs
        elif secs > 0.0001:
            return '%.1fms' % (secs * 1000.0)
        else:
            return '%.1fus' % (secs * 1000.0 * 1000.0)


def format_times(start, stop):
    deltas = [stop[idx] - start[idx] for idx in range(len(stop))]
    if not deltas[2]:
        return 'elapsed: %s, pcpu: ---s' % format_seconds(deltas[2])
    return 'elapsed: %s, pcpu: %d%%' % (format_seconds(deltas[2]), 100.0 * sum(deltas[:-1]) / deltas[2])


class TimingTextTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super(TimingTextTestResult, self).__init__(*args, **kwargs)
        self._timings = {}  # test => timing

    def startTest(self, test):
        super(TimingTextTestResult, self).startTest(test)
        self._timings[test] = take_times()

    def addSuccess(self, test):
        stop_times = take_times()
        super(unittest.TextTestResult, self).addSuccess(test)
        if self.showAll:
            self.stream.writeln("ok\t(%s)" % format_times(self._timings[test], stop_times))
        elif self.dots:
            self.stream.write('.')
            self.stream.flush()


class TimingTextTestRunner(unittest.TextTestRunner):
    resultclass = TimingTextTestResult

    def __init__(self, *args, **kwargs):
        if len(args) < 3 and 'verbosity' not in kwargs:
            kwargs['verbosity'] = 3
        super(TimingTextTestRunner, self).__init__(*args, **kwargs)
