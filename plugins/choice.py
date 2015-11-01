class Analyzer:
    def trace(self, result, ms, event, target, values):
        if event == 'choice':
            choice_id = target
            option_id = values[0]

            if 'choices' not in result:
                result['choices'] = {}

            choices = result['choices']
            if choice_id not in choices:
                choices[choice_id] = {}

            if option_id not in choices[choice_id]:
                choices[choice_id][option_id] = 0

            choices[choice_id][option_id] += 1
