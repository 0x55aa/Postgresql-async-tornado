# Postgresql-async-tornado
A module for asynchronous PostgreSQL queries,works with Tornado and psycopg2.

对psycopg2的简单封装


Usage
-----

```python
import ptdb as db

...

class MainHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        mid = self.get_argument("m", '')
        r = yield db.query(
            """SELECT * FROM foo WHERE id > %s
            """, (mid, ))

        context = dict(r=r)
        self.render("result.html", **context)

...

```


Contributors
------------
[torndb](https://github.com/bdarnell/torndb)

[momoko](https://github.com/FSX/momoko)
