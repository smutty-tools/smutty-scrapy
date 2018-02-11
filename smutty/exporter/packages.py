from smutty.models import Image, Video


class Package:

    def __init__(self, block, item_class):
        self._block = block
        self._item_class = item_class

    def __repr__(self):
        return "{0}({_block}, {_item_class.__name__})".format(self.__class__.__name__, **self.__dict__)

    def db_items(self, db_session, sorted_by_id=False):
        result = self._block.items(db_session, self._item_class)
        if sorted_by_id:
            result = result.order_by(self._item_class.item_id)
        return result

    def name(self):
        return "{0}-{1}-{2}".format(
            self._item_class.__name__.lower(),
            self._block.min_id,
            self._block.max_id)


class ImagePackage(Package):

    def __init__(self, block):
        super().__init__(block, Image)


class VideoPackage(Package):

    def __init__(self, block):
        super().__init__(block, Video)
