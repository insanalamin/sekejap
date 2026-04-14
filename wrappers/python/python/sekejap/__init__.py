from .sekejap import DB as _NativeDB, Hit, EdgeHit
from ._dataframe import DataFrameAccessor

__all__ = ["DB", "Hit", "EdgeHit", "DataFrameAccessor"]


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

    Graph aggregate::

        hits = db.query("""
            SELECT b._key AS venue, COUNT(a) AS performances
            FROM MATCH (a:bands)-[r:played_at]->(b:venues)
            GROUP BY b._key ORDER BY performances DESC LIMIT 10
        """)

    PATH_* aggregates (operate on path intrinsic arrays)::

        hits = db.query("""
            SELECT b._key AS dest, PATH_PRODUCT(r._path_strength) AS reliability
            FROM MATCH (a:venues)-[r:route_to*1..3]->(b:venues)
            WHERE a._key = 'melbourne_cbd'
        """)

    CASE WHEN::

        hits = db.query("""
            SELECT b._key AS venue,
                   CASE WHEN r._depth = 1 THEN 'direct' ELSE 'multi-hop' END AS tier
            FROM MATCH (a:bands)-[r:played_at]->(b:venues)
        """)

    Shortest path (returns a row with path fields, 0 rows if unreachable)::

        hits = db.query("""
            SELECT a.suburb AS from_name, b.suburb AS to_name, r.length AS hops
            FROM MATCH SHORTEST (a)-[r*]->(b)
            WHERE a._key = 'venues/fitzroy_town_hall'
              AND b._key = 'venues/melbourne_cbd'
        """)
        if hits:
            print(f"hops: {json.loads(hits[0].payload)['hops']}")

    Multi-FROM cross-join::

        hits = db.query("""
            SELECT b._key AS venue, e._key AS event
            FROM MATCH ('bands/the_vines')-[:played_at]->(b), events AS e
        """)

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
