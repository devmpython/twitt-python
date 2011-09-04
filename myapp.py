#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
http://devmeetings.pl/trainings/wydajno%C5%9B%C4%87-nodejs-kontra-reszta-%C5%9Bwiata
"""

from tornado import ioloop, web, httpserver, database
import json
import datetime
import operator


class Application(web.Application):
    def __init__(self):
        handlers = (
            (r"/statuses/(?P<req_type>home|user)_timeline.json", TimelineHandler),
            (r"/statuses/update.json", UpdateHandler),
        )

        self._dbs = (
            database.Connection(host="10.1.1.10",
                                user="devcamp",
                                password="devcamp",
                                database="twitter1"),
            database.Connection(host="10.1.1.10",
                                user="devcamp",
                                password="devcamp",
                                database="twitter2"),
            database.Connection(host="10.1.1.10",
                                user="devcamp",
                                password="devcamp",
                                database="twitter3"),
            database.Connection(host="10.1.1.10",
                                user="devcamp",
                                password="devcamp",
                                database="twitter4"),
        )
        super(Application, self).__init__(handlers)

    def get_user_id(self, username):
        """Tries to find user with such name in all databases."""
        row = None
        for db in self._dbs:
            row = db.get("SELECT id FROM users WHERE screen_name=%s LIMIT 1", username)
            if row:
                return row["id"]
        raise None

    def _get_db(self, user_id):
        """Get the right db instance (partition) based on user_id index."""
        return self._dbs[(user_id - 1) % 4]


    def user_statuses(self, user_id, limit=20):
        """Returns user statuses."""
        query = """
        SELECT id, created_at, text
        FROM statuses
        WHERE user_id=%s
        ORDER BY created_at DESC
        LIMIT %s
        """
        db = self._get_db(user_id)

        return [{
            "id": row["id"],
            "created_at": row["created_at"].isoformat(),
            "text": row["text"],
        } for row in db.iter(query, user_id, limit)]

    def __friends_ids(self, user_id):
        query = """
        SELECT follower_id AS id
        FROM followers
        WHERE user_id=%s"""
        db = self._get_db(user_id)
        friends_ids = (set(), set(), set(), set())
        for row in db.iter(query, user_id):
            friends_ids[(row["id"] - 1) % 4].add(row["id"])
        return friends_ids

    def friends_statuses(self, user_id, limit=20):
        """Returns friends statuses (combined with user statues)"""
        query_template = """
        SELECT statuses.created_at AS created_at,
               statuses.text AS text,
               statuses.id AS stat_id,
               users.name AS user_name,
               user_id,
               screen_name
        FROM statuses JOIN users ON user_id=users.id
        WHERE user_id IN (%s)
        ORDER BY created_at DESC
        LIMIT %s
        """
        ids = self.__friends_ids(user_id)
        ids[(user_id - 1) % 4].add(user_id)

        rows = []

        for i, db in enumerate(self._dbs):
            if not ids[i]:
                continue
            query = query_template % (",".join("%s" for i in ids[i]), "%s")
            params = list(ids[i])
            params.append(limit)
            results = db.query(query, *params)
            if results:
                rows.extend(results)

        if not rows:
            return []

        rows = sorted(rows, reverse=True,
                      key=operator.itemgetter("created_at"))[0:limit]
        return map(lambda row: {
            "created_at": row["created_at"].isoformat(),
            "text": row["text"],
            "id": row["stat_id"],
            "user": {
                "name": row["user_name"],
                "id": row["user_id"],
                "screen_name": row["screen_name"],
            }
        }, rows)

    def add_status(self, user_id, text):
        """Adds status of specific user."""
        query = """
        INSERT INTO statuses (text, user_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
        """
        db = self._get_db(user_id)
        now = datetime.datetime.now()
        status_id = db.execute(query, text, user_id, now, now);

        return {
            "id": status_id,
            "created_at": now
        }


class TimelineHandler(web.RequestHandler):
    """ doc """
    def compute_etag(self):
        return None

    SUPPORTED_METHODS = ("GET",)

    def get(self, req_type):
        screen_name = self.get_argument("screen_name")
        user_id = self.application.get_user_id(screen_name)
        if not user_id:
            raise web.HTTPError(404)

        statuses = None
        if req_type == "home":
            statuses = self.application.friends_statuses(user_id)
        else:
            statuses = self.application.user_statuses(user_id)

        if statuses:
            self.write(json.dumps(statuses, separators=(',', ':')))
            self.set_header("Content-type", "application/json; charset=UTF-8")
        else:
            self.set_status(204)


class UpdateHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("POST",)

    def post(self):
        screen_name = self.get_argument("screen_name")
        status_text = self.get_argument("status")
        user_id = self.application.get_user_id(screen_name)
        if not user_id:
            raise web.HTTPError(404)

        result = self.application.add_status(screen_name, status_text)
        self.write(result)


if __name__ == "__main__":
    http_server = httpserver.HTTPServer(Application())
    http_server.listen(8888)
    # TODO: figure out what causes the problems with db connections
    #       when forking several worker processes
    # http_server.bind(8888)
    # http_server.start(0)
    ioloop.IOLoop.instance().start()