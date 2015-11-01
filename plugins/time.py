class Analyzer:
    def trace(self, result, ms, event, target, values):
        if 'startTime' not in result:
            result['startTime'] = ms

        result['duration'] = ms.timestamp() - result['startTime'].timestamp()
