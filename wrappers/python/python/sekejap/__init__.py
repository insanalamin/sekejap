from .sekejap import DB as _NativeDB, Hit
from ._dataframe import DataFrameAccessor

__all__ = ["DB", "Hit", "DataFrameAccessor"]


class DB(_NativeDB):
    """
    sekejap embedded database.

    Example::

        from sekejap import DB

        db = DB()                   # in-memory
        db = DB("./data")           # persistent (WAL-backed)

        db.put("items/1", '{\"_collection\":\"items\",\"_key\":\"1\",\"name\":\"foo\"}')
        hits = db.query("SELECT * FROM items")

        # pandas / dataframe integration
        df = db.df.query("SELECT * FROM items")
        db.df.load_nodes(df, "items")
    """

    @property
    def df(self) -> DataFrameAccessor:
        """Pandas / dataframe integration namespace (``db.df``)."""
        try:
            return self._df_accessor
        except AttributeError:
            self._df_accessor = DataFrameAccessor(self)
            return self._df_accessor
