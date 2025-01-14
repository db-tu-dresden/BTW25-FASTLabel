def get_equality_information(entry):
    key = 'eq'
    equal_statement = entry[key]
    try:
        alias, column = equal_statement[0].split('.')
        value = equal_statement[1]['literal']
    except (TypeError, ValueError):
        # Join
        # logger = get_logger()
        # logger.info(f'Equality statement not recognized in {equal_statement}')
        alias, column, key, value = [None] * 4

    return alias, column, key, value
