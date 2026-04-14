from .sekejap import DB as _NativeDB, Hit, EdgeHit, PathResult
from ._dataframe import DataFrameAccessor

__all__ = ["DB", "Hit", "EdgeHit", "PathResult", "DataFrameAccessor"]


class DB(_NativeDB):
    """
    sekejap embedded database.

    Open / create::

        from sekejap import DB
        import json

        db = DB()                   # in-memory
        db = DB("./data")           # persistent (WAL-backed)

    Basic node and edge operations::

        db.put("venues/fitzroy_town_hall",
               '{"_collection":"venues","_key":"fitzroy_town_hall","suburb":"Fitzroy"}')
        db.link("bands/the_vines", "venues/fitzroy_town_hall", "played_at", 1.0)

    Standard SELECT::

        hits = db.query("SELECT * FROM venues WHERE suburb = 'Fitzroy'")
        for h in hits:
            print(json.loads(h.payload))

    Graph aggregate — RETURN form::

        hits = db.query(\"\"\"
            MATCH (a:bands)-[r:played_at]->(b:venues)
            RETURN b._key AS venue, COUNT(a) AS performances
            GROUP BY b._key ORDER BY performances DESC LIMIT 10
        \"\"\")

    Graph aggregate — SELECT FROM MATCH form (identical result)::

        hits = db.query(\"\"\"
            SELECT b._key AS venue, COUNT(a) AS performances
            FROM MATCH (a:bands)-[r:played_at]->(b:venues)
            GROUP BY b._key ORDER BY performances DESC LIMIT 10
        \"\"\")

    PATH_* aggregates (operate on path intrinsic arrays)::

        hits = db.query(\"\"\"
            MATCH (a:venues)-[r:route_to*1..3]->(b:venues)
            WHERE a._key = 'melbourne_cbd'
            RETURN b._key AS dest, PATH_PRODUCT(r._path_strength) AS reliability
        \"\"\")

    CASE WHEN::

        hits = db.query(\"\"\"
            MATCH (a:bands)-[r:played_at]->(b:venues)
            RETURN b._key AS venue,
                   CASE WHEN r._depth = 1 THEN 'direct' ELSE 'multi-hop' END AS tier
        \"\"\")

    Time functions::

        hits = db.query(\"\"\"
            MATCH (a:bands)-[r:played_at]->(b:venues)
            RETURN b._key AS venue, NOW() AS ts, AGE_DAYS(a.founded) AS age_days
        \"\"\")

    Shortest path::

        result = db.path_query(
            "MATCH SHORTEST (a)-[r*]->(b) WHERE a._key = 'venues/fitzroy_town_hall'"
            " AND b._key = 'venues/melbourne_cbd'"
        )
        if result:
            print(f"hops: {result.length}")

    Introspection::

        hits = db.show("SHOW TABLES")                  # [{name, count}, ...]
        hits = db.show("SHOW EDGES")                   # [{from, type, to, count}, ...]
        hits = db.show("SHOW EDGES FROM bands")        # [{from, type, count}, ...]
        hits = db.show("SHOW venues")                  # [{field, type, source}, ...]
        for h in hits:
            print(json.loads(h.payload))

    Pandas / dataframe integration::

        df = db.df.query("SELECT * FROM venues")
        db.df.load_nodes(df, "venues")
    """

    @property
    def df(self) -> DataFrameAccessor:
        """Pandas / dataframe integration namespace (``db.df``)."""
        try:
            return self._df_accessor
        except AttributeError:
            self._df_accessor = DataFrameAccessor(self)
            return self._df_accessor
