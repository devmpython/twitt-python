#!/usr/bin/env python
from tornado import ioloop, web, httpserver, database

class Application(web.Application):
    def __init__(self):
        handlers = [
            (r"/statuses/(?P<type>home|user)_timeline.json", TimelineHandler),
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
                
    def friends_statuses(self, username, limit):
        user_id = self.get_user_id(username)
        db = self.dbs[user_id % len(self.dbs)]
        u = db.iter("SELECT follower_id AS id from followers "\
                                 "WHERE user_id=%s", user_id)
    	return u

    def user_statuses(self, username, limit=10):
        user_id = self.get_user_id(username)
        db = self.dbs[user_id % len(self.dbs)]
        rows = db.iter("SELECT id, created_at, text FROM statuses "
                       "WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
                       user_id, limit)
        result = [];
        for row in rows:
            rows.insert({
                "key": "value"
            })
           
class TimelineHandler(web.RequestHandler):

    SUPPORTED_METHODS = ("GET",)
    
    def get(self, type):
        screen_name = self.get_argument("screen_name")
        if type is "home":
            statuses = self.application.fiends_statuses(screen_name)
        else:
            statuses = self.application.user_statuses(screen_name)

        if statuses is None:
            self.set_status(204)
            self.finish()
        else:
            self.write(statuses)


class UpdateHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("POST", )

    def post(self):
        pass

        
if __name__ == "__main__":
    http_server = httpserver.HTTPServer(Application())
    http_server.bind(8888)
    http_server.start(0)
    ioloop.IOLoop.instance().start()

