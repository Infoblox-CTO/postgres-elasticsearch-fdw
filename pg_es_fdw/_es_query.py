try:
    from multicorn import ANY
except ImportError:
    # Don't fail the import if we're running this directly.
    ANY = object()


_RANGE_OPS = {
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
}


def _base_qual_to_es(col, op, value, column_map=None):
    if column_map:
        col = column_map.get(col, col)

    if value is None:
        if op == "=":
            return {"bool": {"must_not": {"exists": {"field": col}}}}
        if op in ["<>", "!="]:
            return {"exists": {"field": col}}
        # Weird comparison to a NULL
        return {"match_all": {}}

    if op in _RANGE_OPS:
        return {"range": {col: {_RANGE_OPS[op]: value}}}

    if op == "=":
        return {"term": {col: value}}

    if op in ["<>", "!="]:
        return {"bool": {"must_not": {"term": {col: value}}}}

    if op == "~~":
        return {"match": {col: value.replace("%", "*")}}

    # For unknown operators, get everything
    return {"match_all": {}}


def _qual_to_es(qual, column_map=None):
    if qual.is_list_operator:
        if qual.list_any_or_all == ANY:
            # Convert col op ANY([a,b,c]) into (cop op a) OR (col op b)...
            return {
                "bool": {
                    "should": [
                        _base_qual_to_es(
                            qual.field_name, qual.operator[0], v, column_map
                        )
                        for v in qual.value
                    ]
                }
            }
        # Convert col op ALL(ARRAY[a,b,c...]) into (cop op a) AND (col op b)...
        return {
            "bool": {
                "must": [
                    _base_qual_to_es(qual.field_name, qual.operator[0], v, column_map)
                    for v in qual.value
                ]
            }
        }
    else:
        return _base_qual_to_es(
            qual.field_name, qual.operator, qual.value, column_map
        )


def quals_to_es(quals, aggs=None, ignore_columns=None, column_map=None):
    """Convert a list of Multicorn quals to an ElasticSearch query"""
    ignore_columns = ignore_columns or []
    if aggs is not None:
        return {
            "aggs": {
                "res": {
                    aggs["operation"]: {
                        "field": aggs["column"]
                    }
                }
            }
        }
    return {
        "query": {
            "bool": {
                "must": [
                    _qual_to_es(q, column_map)
                    for q in quals
                    if q.field_name not in ignore_columns
                ]
            }
        }
    }
