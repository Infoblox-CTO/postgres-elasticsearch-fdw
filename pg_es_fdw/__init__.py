""" Elastic Search foreign data wrapper """
# pylint: disable=too-many-instance-attributes, import-error, unexpected-keyword-arg, broad-except, line-too-long

import json
import logging
from datetime import datetime

from pytz import timezone

from elasticsearch import VERSION as ELASTICSEARCH_VERSION
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log2pg

from ._es_query import _PG_TO_ES_AGG_FUNCS, _OPERATORS_SUPPORTED, quals_to_es


class ElasticsearchFDW(ForeignDataWrapper):
    """ Elastic Search Foreign Data Wrapper """

    @property
    def rowid_column(self):
        """Returns a column name which will act as a rowid column for
        delete/update operations.

        This can be either an existing column name, or a made-up one. This
        column name should be subsequently present in every returned
        resultset."""

        return self._rowid_column

    def __init__(self, options, columns):
        super(ElasticsearchFDW, self).__init__(options, columns)

        self.index = options.pop("index", "")
        self.doc_type = options.pop("type", "")
        self.query_column = options.pop("query_column", None)
        self.score_column = options.pop("score_column", None)
        self.scroll_size = int(options.pop("scroll_size", "1000"))
        self.scroll_duration = options.pop("scroll_duration", "10m")
        self._rowid_column = options.pop("rowid_column", "id")
        username = options.pop("username", None)
        password = options.pop("password", None)
        isopensearch = options.pop("opensearch", False)

        if ELASTICSEARCH_VERSION[0] >= 7:
            self.path = "/{index}".format(index=self.index)
            self.arguments = {"index": self.index}
        else:
            self.path = "/{index}/{doc_type}".format(
                index=self.index, doc_type=self.doc_type
            )
            self.arguments = {"index": self.index, "doc_type": self.doc_type}

        if (username is None) != (password is None):
            raise ValueError("Must provide both username and password")
        if username is not None:
            auth = (username, password)
        else:
            auth = None

        scheme = options.pop("scheme", "https")
        host = options.pop("host", "localhost")
        port = int(options.pop("port", "9200"))
        timeout = int(options.pop("timeout", "10"))
        if isopensearch:
            self.client = OpenSearch(
                [{"scheme": scheme, "host": host, "port": port}], http_auth=auth, timeout=timeout, **options
            )
        else:
            self.client = Elasticsearch(
                [{"scheme": scheme, "host": host, "port": port}], http_auth=auth, timeout=timeout, **options
            )

        self.columns = columns
        self.json_columns = {
            column.column_name
            for column in columns.values()
            if column.base_type_name.upper() in {"JSON", "JSONB"}
        }

        self.scroll_id = None

    def get_rel_size(self, quals, columns):
        """Helps the planner by returning costs.
        Returns a tuple of the form (number of rows, average row width)"""

        try:
            query, _ = self._get_query(quals)

            if query:
                response = self.client.count(body=query, **self.arguments)
            else:
                response = self.client.count(**self.arguments)
            return (response["count"], len(columns) * 100)
        except Exception as exception:
            log2pg(
                "COUNT for {path} failed: {exception}".format(
                    path=self.path, exception=exception
                ),
                logging.ERROR,
            )
            return (0, 0)

    def can_pushdown_upperrel(self):
        return {
            "groupby_supported": True,
            "agg_functions": list(_PG_TO_ES_AGG_FUNCS),
            "operators_supported": _OPERATORS_SUPPORTED,
        }

    def explain(
        self,
        quals,
        columns,
        sortkeys=None,
        aggs=None,
        group_clauses=None,
        verbose=False,
    ):
        query, _ = self._get_query(quals, aggs=aggs, group_clauses=group_clauses)
        return [
            "Elasticsearch query to %s" % self.client,
            "Query: %s" % json.dumps(query, indent=4),
        ]

    def execute(self, quals, columns, aggs=None, group_clauses=None):
        """ Execute the query """

        try:
            query, query_string = self._get_query(
                quals, aggs=aggs, group_clauses=group_clauses
            )

            is_aggregation = aggs or group_clauses

            if query:
                response = self.client.search(
                    size=self.scroll_size if not is_aggregation else 0,
                    scroll=self.scroll_duration if not is_aggregation else None,
                    body=query,
                    **self.arguments
                )
            else:
                response = self.client.search(
                    size=self.scroll_size, scroll=self.scroll_duration, **self.arguments
                )

            if not response["hits"]["hits"] and not is_aggregation:
                return

            if is_aggregation:
                yield from self._handle_aggregation_response(
                    query, response, aggs, group_clauses
                )
                return

            while True:
                self.scroll_id = response["_scroll_id"]

                for result in response["hits"]["hits"]:
                    yield self._convert_response_row(result, columns, query_string)

                if len(response["hits"]["hits"]) < self.scroll_size:
                    return
                response = self.client.scroll(
                    scroll_id=self.scroll_id, scroll=self.scroll_duration
                )
        except Exception as exception:
            log2pg(
                "SEARCH for {path} failed: {exception}".format(
                    path=self.path, exception=exception
                ),
                logging.ERROR,
            )
            return

    def end_scan(self):
        if self.scroll_id:
            self.client.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None

    def insert(self, new_values):
        """ Insert new documents into Elastic Search """

        if self.rowid_column not in new_values:
            log2pg(
                'INSERT requires "{rowid}" column. Missing in: {values}'.format(
                    rowid=self.rowid_column, values=new_values
                ),
                logging.ERROR,
            )
            return (0, 0)

        document_id = new_values[self.rowid_column]
        new_values.pop(self.rowid_column, None)

        for key in self.json_columns.intersection(new_values.keys()):
            new_values[key] = json.loads(new_values[key])

        try:
            response = self.client.index(
                id=document_id, body=new_values, **self.arguments
            )
            return response
        except Exception as exception:
            log2pg(
                "INDEX for {path}/{document_id} and document {document} failed: {exception}".format(
                    path=self.path,
                    document_id=document_id,
                    document=new_values,
                    exception=exception,
                ),
                logging.ERROR,
            )
            return (0, 0)

    def update(self, document_id, new_values):
        """ Update existing documents in Elastic Search """

        new_values.pop(self.rowid_column, None)

        for key in self.json_columns.intersection(new_values.keys()):
            new_values[key] = json.loads(new_values[key])

        try:
            response = self.client.index(
                id=document_id, body=new_values, **self.arguments
            )
            return response
        except Exception as exception:
            log2pg(
                "INDEX for {path}/{document_id} and document {document} failed: {exception}".format(
                    path=self.path,
                    document_id=document_id,
                    document=new_values,
                    exception=exception,
                ),
                logging.ERROR,
            )
            return (0, 0)

    def delete(self, document_id):
        """ Delete documents from Elastic Search """

        try:
            response = self.client.delete(id=document_id, **self.arguments)
            return response
        except Exception as exception:
            log2pg(
                "DELETE for {path}/{document_id} failed: {exception}".format(
                    path=self.path, document_id=document_id, exception=exception
                ),
                logging.ERROR,
            )
            return (0, 0)

    def _get_query(self, quals, aggs=None, group_clauses=None):
        ignore_columns = []
        if self.query_column:
            ignore_columns.append(self.query_column)
        if self.score_column:
            ignore_columns.append(self.score_column)

        query = quals_to_es(
            quals,
            aggs=aggs,
            group_clauses=group_clauses,
            ignore_columns=ignore_columns,
            column_map={self._rowid_column: "_id"} if self._rowid_column else None,
        )

        if group_clauses is not None:
            # Configure pagination for GROUP BY's
            query["aggs"]["group_buckets"]["composite"]["size"] = self.scroll_size

        if not self.query_column:
            return query, None

        query_string = next(
            (
                qualifier.value
                for qualifier in quals
                if qualifier.field_name == self.query_column
            ),
            None,
        )

        if query_string:
            query["query"]["bool"]["must"].append(
                {"query_string": {"query": query_string}}
            )

        return query, query_string

    def _convert_response_row(self, row_data, columns, query):
        if query:
            # Postgres checks the query after too, so the query column needs to be present
            return dict(
                [
                    (column, self._convert_response_column(column, row_data))
                    for column in columns
                    if column in row_data["_source"]
                    or column == self.rowid_column
                    or column == self.score_column
                ]
                + [(self.query_column, query)]
            )
        return {
            column: self._convert_response_column(column, row_data)
            for column in columns
            if column in row_data["_source"]
            or column == self.rowid_column
            or column == self.score_column
        }

    def _convert_response_column(self, column, row_data):
        if column == self.rowid_column:
            return row_data["_id"]
        if column == self.score_column:
            return row_data["_score"]
        value = row_data["_source"][column]
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        column_def = self.columns.get(column, None)
        if column_def is None:
            return value

        if column_def.base_type_name.startswith("timestamp"):
            if isinstance(value, int):
                if value > 3000000000:
                    value = value / 1000.0
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
        return value

    def _handle_aggregation_response(self, query, response, aggs, group_clauses):
        if group_clauses is None:
            result = {}

            for agg_name in aggs:
                if agg_name == "count.*":
                    # COUNT(*) is a special case, since it doesn't have a
                    # corresponding aggregation primitive in ES
                    result[agg_name] = response["hits"]["total"]["value"]
                    continue

                result[agg_name] = response["aggregations"][agg_name]["value"]
            yield result
        else:
            while True:
                for bucket in response["aggregations"]["group_buckets"]["buckets"]:
                    result = {}

                    for column in group_clauses:
                        result[column] = bucket["key"][column]

                    if aggs is not None:
                        for agg_name in aggs:
                            if agg_name == "count.*":
                                # In general case with GROUP BY clauses COUNT(*)
                                # is taken from the bucket's doc_count field
                                result[agg_name] = bucket["doc_count"]
                                continue

                            result[agg_name] = bucket[agg_name]["value"]

                    yield result

                # Check if we need to paginate results
                if "after_key" not in response["aggregations"]["group_buckets"]:
                    break

                query["aggs"]["group_buckets"]["composite"]["after"] = response[
                    "aggregations"
                ]["group_buckets"]["after_key"]

                response = self.client.search(size=0, body=query, **self.arguments)
