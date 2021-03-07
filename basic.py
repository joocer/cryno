import orjson as json
from validator import *

def read_file(filename, chunk_size=32*1024*1024, delimiter="\n"):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(filename, "r", encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward

def read_jsonl(filename, limit=-1, chunk_size=32*1024*1024, delimiter="\n"):
    """"""
    file_reader = read_file(filename, chunk_size=chunk_size, delimiter=delimiter)
    line = next(file_reader, None)
    while line:
        yield json.loads(line)
        limit -= 1
        if limit == 0:
            return
        try:
            line = next(file_reader)
        except StopIteration:
            return

schema = Schema('twitter.schema')
data = list(read_jsonl("twitter.jsonl"))

def get_type(validators):

    val = [type(v).__name__ for v in validators if type(v).__name__ != 'function']
    if len(val) == 0:
        val = [v.__name__ for v in validators if v.__name__ != 'is_null']
    
    try:
        val = val.pop()
    except:
        val = "other"
    
    if val in ['is_numeric']:
        return "numeric"
    if val in ['is_string', 'is_cve']:
        return "string"
    if val in ['is_enum', 'is_boolean']:
        return "enum"
    if val in ['is_date']:
        return "date"

    return "other"

bar_chars = (' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█')

def _draw_histogram(bins):
    mx = max([v for k,v in bins.items()])
    bar_height = (mx / 8)
    if mx == 0:
        return ' ' * len(bins)
    
    histogram = ''
    for k,v in bins.items():
        height = int(v / bar_height)
        histogram += bar_chars[height]
        
    return histogram

def _redistribute_bins(bins, number_of_bins=100):
    
    mn = min([l for l,h in bins])
    mx = max([h for l,h in bins])
    
    bin_size = (mx - mn) // number_of_bins
    
    new_bins = {}
    for counter in range(number_of_bins - 1):
        new_bins[(mn + (bin_size * counter), mn + (bin_size * (counter + 1)) - 1)] = 0
    new_bins[(mn + (bin_size * (counter + 1)),mx)] = 0
        
    for old_bounds in bins:
        old_lower, old_upper = old_bounds
        old_mid = (old_lower + old_upper) // 2
        binned = False
        for new_bounds in new_bins:
            new_lower, new_upper = new_bounds
            if old_mid >= new_lower and old_mid <= new_upper:
                new_bins[new_bounds] += bins[old_bounds]
                binned = True
                break
        if binned == False:
            print('miss', old_mid, mn, mx)
            
    return new_bins

def _date_from_epoch(seconds, form='%Y-%m-%d %H:%M:%S'):
    import datetime
    return datetime.datetime.fromtimestamp(seconds).strftime(form)

MAXIMUM_UNIQUE_VALUES = 100000

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
                
            if field_summary.get('bins') is None:
                field_summary['bins'] = {}
            binned = False
            for bounds in field_summary['bins']:
                bottom, top = bounds
                if field_value >= bottom and field_value <= top:
                    field_summary['bins'][bounds] += 1
                    binned = True
            if not binned:
                field_summary['bins'][(field_value, field_value)] = 1
                
            if len(field_summary['bins']) > 500:
                field_summary['bins'] = _redistribute_bins(field_summary['bins'], 50)
                
        elif field_type == 'date':
            # convert to epoch seconds
            from dateutil import parser
            field_value = int(parser.parse(field_value).timestamp())
            
            if field_summary.get('max', field_value) <= field_value:
                field_summary['max'] = field_value
            if field_summary.get('min', field_value) >= field_value:
                field_summary['min'] = field_value
                
            if field_summary.get('bins') is None:
                field_summary['bins'] = {}
            binned = False
            for bounds in field_summary['bins']:
                bottom, top = bounds
                if field_value >= bottom and field_value <= top:
                    field_summary['bins'][bounds] += 1
                    binned = True
            if not binned:
                field_summary['bins'][(field_value, field_value)] = 1
                
            if len(field_summary['bins']) > 500:
                field_summary['bins'] = _redistribute_bins(field_summary['bins'], 50)

        elif field_type == 'string':
            if field_summary.get('max_length', len(field_value)) <= len(field_value):
                field_summary['max_length'] = len(field_value)
            if field_summary.get('unique_value_list') is None:
                field_summary['unique_value_list'] = {hash(field_value)}
            elif len(field_summary['unique_value_list']) < MAXIMUM_UNIQUE_VALUES:
                field_summary['unique_value_list'].add(hash(field_value))
            field_summary['unique_values'] = len(field_summary['unique_value_list'])

        elif field_type == 'enum':
            if field_summary.get('values') is None:
                field_summary['values'] = {}
            field_summary['values'][field_value] = field_summary['values'].get(field_value, 0) + 1

[summary[k]['unique_value_list'].clear() for k in schema._validators if 'unique_value_list' in summary[k]]
new_bins = [(k, _redistribute_bins(summary[k]['bins'], 10)) for k in summary if 'bins' in summary[k]]
for k, bins in new_bins:
    summary[k]['bins'] = bins
    
date_fields = [k for k in summary if summary[k]['type'] == 'date']
for date_field in date_fields:
    summary[date_field]['min'] = _date_from_epoch(summary[date_field]['min'])
    summary[date_field]['max'] = _date_from_epoch(summary[date_field]['max'])
    new_bins = {}
    for bound in summary[date_field]['bins']:
        bottom, top = bound
        bottom = _date_from_epoch(bottom)
        top = _date_from_epoch(top)
        new_bins[(bottom, top)] = summary[date_field]['bins'][bound]
    summary[date_field]['bins'] = new_bins

from pprint import pprint
#pprint(summary)

def enum_summary(dic):
    s = {k:v for k,v in sorted(dic.items(), key=lambda item: item[1], reverse=True)}
    cumsum = sum([v for k,v in dic.items()])
    eliminated = 0
    result = ''
    for index, item in enumerate(s):
        if index == 2:
            break
        result += F"`{item}`: {(s[item] / cumsum):.1%} "
        eliminated += s[item]
    if eliminated < cumsum:
        result += F"[other]: {(cumsum - eliminated) / cumsum:.1%}"
    return result

def human_format(num):
    display = float('{:.2g}'.format(num))
    magnitude = 0
    while abs(display) >= 1000:
        magnitude += 1
        display /= 1000.0
    if magnitude < 2:
        return str(num)
    return '{}{}'.format('{:1f}'.format(display).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T', 'P', 'E', 'Z', 'Y', 'Br'][magnitude])


for field in summary:
    field_summary = summary[field]
    if field_summary['type'] == 'numeric':
        print(F"[num] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} [range] {human_format(field_summary['min'])} to {human_format(field_summary['max'])} [mean] {field_summary['mean']:.2} >{_draw_histogram(field_summary['bins'])}<") 
    if field_summary['type'] == 'date':
        print(F"[num] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} [range] {field_summary['min']} to {field_summary['max']} >{_draw_histogram(field_summary['bins'])}<") 
    if field_summary['type'] == 'other':
        print(F"[oth] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%}")
    if field_summary['type'] == 'string':
        print(F"[str] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} [unique] {field_summary['unique_values']}")
    if field_summary['type'] == 'enum':
        print(F"[enm] {field:20} [count] {field_summary['items']} [empty] {(field_summary['nulls'] / field_summary['items']):.1%} {enum_summary(field_summary['values'])}" )