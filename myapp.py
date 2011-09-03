#!/usr/bin/env python
from tornado import ioloop, web, httpserver, database
import json
import datetime
import operator

class Application(web.Application):
    def __init__(self):
        handlers = [
            (r"/statuses/(?P<req_type>home|user)_timeline.json", TimelineHandler),
            (r"/statuses/update.json", UpdateHandler),
        ]

        self.dbs = [
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
        ]
        web.Application.__init__(self, handlers)

    def get_user_id(self, username):
        """Tries to find user with such name in all databases."""
        row = None
        for db in self.dbs:
            row = db.get("SELECT id FROM users WHERE screen_name=%s LIMIT 1", username)
            if row:
                return row["id"]
        raise Exception("user not found")

    def get_db(self, user_id):
        return self.dbs[(user_id-1) % 4]

    def friends_statuses(self, username, limit=20):
        user_id = self.get_user_id(username)
        db = self.get_db(user_id)
        ids = (set(), set(), set(), set())
        for row in db.iter("SELECT follower_id AS id from followers "
                           "WHERE user_id=%s", user_id):
            ids[(row["id"] - 1) % 4].add(row["id"])

        query = """
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
        rows = []
        
        for i, db in enumerate(self.dbs):
            if not ids[i]:
                continue
            q_str = query % (",".join("%s" for i in xrange(len(ids[i]))), "%s")
            params = list(ids[i])
            params.append(limit)
            rows.extend(db.query(q_str, *params))

        return map(lambda row: {
            "created_at": row["created_at"].isoformat(),
            "text": row["text"],
            "id": row["stat_id"],
            "user": {
                "name": row["user_name"],
                "id": row["user_id"],
                "screen_name": row["screen_name"],
            }
        }, sorted(rows, reverse=True, key=operator.itemgetter("created_at"))[0:limit])

    def user_statuses(self, username, limit=20):
        user_id = self.get_user_id(username)
        db = self.get_db(user_id)
        rows = db.iter("SELECT id, created_at, text FROM statuses "
                       "WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
                       user_id, limit)
        result = []
        for row in rows:
            result.append({
                "id": row["id"],
                "created_at": row["created_at"].isoformat(), # .strftime("%a %b %d %H:%M:%S %z %Y"),
                "text": row["text"]
            })
        return result
        
    def add_status(self, screen_name, status):
        user_id = self.get_user_id(screen_name)
        db = self.get_db(user_id)
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated_at = created_at
        
        status_id = db.execute("INSERT INTO statuses VALUES(NULL, %s, %s, %s, %s)", status, user_id, created_at, updated_at);
        
        return {
            "id": status_id,
            "created_at": created_at
        }

        
class TimelineHandler(web.RequestHandler):
    """ doc """
    def compute_etag(self):
        return None

    SUPPORTED_METHODS = ("GET",)

    def get(self, req_type):
        screen_name = self.get_argument("screen_name")
        if req_type == "home":
            statuses = self.application.friends_statuses(screen_name)
        else:
            statuses = self.application.user_statuses(screen_name)

        if statuses:
            self.write(json.dumps(statuses))
            self.set_header("Content-type", "application/json; charset=UTF-8")
        else:
            self.set_status(204)
        self.finish()


class UpdateHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("POST", )

    def post(self):
        screen_name = self.get_argument("screen_name")
        result = self.application.add_status(screen_name, self.get_argument('status'))
        self.write(result)

if __name__ == "__main__":
    http_server = httpserver.HTTPServer(Application())
    http_server.bind(8888)
    http_server.start(0)
    ioloop.IOLoop.instance().start()