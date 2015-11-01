class Analyzer:
    def trace(self, result, ms, event, target, values):
        if event == 'var':
            var_name = target
            var_value = values[0]

            try:
                var_value = float(var_value)
            except Exception:
                pass

            if 'vars' not in result:
                result['vars'] = {}

            result['vars'][var_name] = var_value
