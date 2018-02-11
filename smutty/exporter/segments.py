class Interval:

    def __init__(self, min_id, max_id):
        assert min_id <= max_id
        self.min_id = min_id
        self.max_id = max_id

    def __repr__(self):
        return "{0}({min_id}, {max_id})".format(self.__class__.__name__, **self.__dict__)


class Block(Interval):

    SIZE = 10000

    def __init__(self, min_id, max_id):
        super().__init__(min_id, max_id)

    @classmethod
    def blocks_covering_interval(cls, interval):
        assert cls.SIZE > 0
        lowest_id = interval.min_id - interval.min_id % cls.SIZE
        highest_id = interval.max_id - interval.max_id % cls.SIZE
        for base in range(lowest_id, highest_id + 1, cls.SIZE):
            yield Block(base, base + cls.SIZE - 1)

    def items(self, db_session, item_class):
        # query is sorted so that exporter output is stable
        return db_session.query(item_class).filter(
                self.min_id <= item_class.item_id,
                item_class.item_id <= self.max_id
            ).order_by(item_class.item_id)
