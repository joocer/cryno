summary = {}

for field in schema._validators:
    summary[field] = {
        "nulls": 0,
        "items": 0,
        "type": get_type(schema._validators[field])
    }

for i, row in enumerate(data):
    if field not in schema._validators:
        print('field not in schema')
    for field in schema._validators:
        field_summary = summary[field]
        field_summary['items'] += 1
        field_type = field_summary['type']
        field_value = row.get(field)
        if field_value is None:
            field_summary['nulls'] += 1

        elif field_type == 'numeric':
            if field_summary.get('max', field_value) <= field_value:
                field_summary['max'] = field_value
            if field_summary.get('min', field_value) >= field_value:
                field_summary['min'] = field_value
            field_summary['cumsum'] = field_summary.get('cumsum', 0) + field_value
            field_summary['mean'] = field_summary['cumsum'] / field_summary['items']

        elif field_type == 'string':
            if field_summary.get('max_length', len(field_value)) <= len(field_value):
                field_summary['max_length'] = len(field_value)

        elif field_type == 'enum':
            field_summary[field_value] = field_summary.get(field_value, 0) + 1
