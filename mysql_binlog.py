#!/usr/bin/env python
# coding=utf-8

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
import json
import sys

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "root"
}


def main():
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=1,
        only_schemas=["etl"],
        only_tables=["test"],
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
    )

    for binlogevent in stream:
        # binlogevent.dump()
        for row in binlogevent.rows:
            # print(row)
            # print(row.values)
            event = {"schema": binlogevent.schema, "table": binlogevent.table}

            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event["values"] = dict(row["values"].items())
                # event = dict(event.items())
            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event["before_values"] = dict(row["before_values"].items())
                event["after_values"] = dict(row["after_values"].items())
                # event = dict(event.items())
            elif isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event["values"] = dict(row["values"].items())
                # event = dict(event.items())

            if "values" in event.keys():
                event["values"]["create_time"] = event["values"]["create_time"].strftime("%Y-%m-%d") if event["values"]["create_time"] else None
            else:
                event["before_values"]["create_time"] = event["before_values"]["create_time"].strftime("%Y-%m-%d") if event["before_values"]["create_time"] else None
                event["after_values"]["create_time"] = event["after_values"]["create_time"].strftime("%Y-%m-%d") if event["after_values"] else None

            # print(event)
            print(json.dumps(event, indent=4))
            # sys.stdout.flush()

    stream.close()


if __name__ == "__main__":
    main()
