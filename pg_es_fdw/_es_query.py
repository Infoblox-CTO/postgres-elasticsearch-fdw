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

_PG_TO_ES_AGG_FUNCS = {
    "avg": "avg",
    "max": "max",
    "min": "min",
    "sum": "sum",
    "count": "value_count",
    "count.*": None  # not mapped to a particular function
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
        return {"wildcard": {col: value.replace("%", "*")}}

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
        return _base_qual_to_es(qual.field_name, qual.operator, qual.value, column_map)


def quals_to_es(
    quals, aggs=None, group_clauses=None, ignore_columns=None, column_map=None
):
    """Convert a list of Multicorn quals to an ElasticSearch query"""
    ignore_columns = ignore_columns or []

    query = {
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

    # Aggregation/grouping queries
    if aggs is not None:
        aggs_query = {
            agg_name: {
                _PG_TO_ES_AGG_FUNCS[agg_props["function"]]: {
                    "field": agg_props["column"]
                }
            }
            for agg_name, agg_props in aggs.items()
            if agg_name != "count.*"
        }

        if group_clauses is None:
            if "count.*" in aggs:
                # There is no particular COUNT(*) equivalent in ES, instead
                # for plain aggregations (e.g. no grouping statements), we need
                # to enable the track_total_hits option in order to get an
                # accuate number of matched docs.
                query["track_total_hits"] = True

            query["aggs"] = aggs_query

    if group_clauses is not None:
        group_query = {
            "group_buckets": {
                "composite": {
                    "sources": [
                        {column: {"terms": {"field": column}}}
                        for column in group_clauses
                    ]
                }
            }
        }

        if aggs is not None:
            group_query["group_buckets"]["aggregations"] = aggs_query

        query["aggs"] = group_query

    return query
