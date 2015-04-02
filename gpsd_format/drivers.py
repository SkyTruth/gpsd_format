#!/usr/bin/env python


import newlinejson
import msgpack
import gzip
try:
    import ujson
    newlinejson.JSON = ujson
except ImportError:
    ujson = None


def get_driver(name):

    """
    Accepts a string and returns the driver class associate dwith that name.
    """

    if name in BaseDriver.by_name and not BaseDriver.by_name[name].compression:
        return BaseDriver.by_name[name]
    else:
        raise ValueError("Unrecognized driver name: %s" % name)


def get_compression(name):

    """
    Accepts a string and returns the driver class associated with that name.
    """

    if name in BaseDriver.by_name and BaseDriver.by_name[name].compression:
        return BaseDriver.by_name[name]
    else:
        raise ValueError("Unrecognized compression name: %s" % name)


def detect_file_type(path):

    """
    Given an file path, attempt to determine appropriate driver.
    """

    ext = path.rpartition('.')[-1]
    if ext in BaseDriver.by_extension and not BaseDriver.by_extension[ext].compression:
        return BaseDriver.by_extension[ext]
    else:
        raise ValueError("Can't determine driver: %s" % path)


def detect_compression_type(path):

    """
    Given a file path, attempt to determine the appropriate compression driver.
    """

    ext = path.rpartition('.')[-1]
    if ext in BaseDriver.by_extension and BaseDriver.by_extension[ext].compression:
        return BaseDriver.by_extension[ext]
    else:
        raise ValueError("Can't determine compression: %s" % path)


class BaseDriver(object):
    by_name = {}
    by_extension = {}

    class __metaclass__(type):
        def __init__(driver, name, bases, members):
            type.__init__(driver, name, bases, members)
            if name != 'BaseDriver':
                driver.register_driver()

        def register_driver(driver):
            driver.validate_driver()
            BaseDriver.by_name[driver.name] = driver
            for ext in driver.extensions:
                BaseDriver.by_extension[ext] = driver

        def validate_driver(driver):
            assert isinstance(driver.name, str)
            assert isinstance(driver.extensions, (tuple, list))
            assert isinstance(driver.modes, (tuple, list))
            for attr in (
                    '__iter__', '__next__', 'read', 'write', 'from_path', 'modes', 'name', 'write', 'close', 'closed', 'read'):
                assert hasattr(driver, attr)

            return True


    def __init__(self, *args, **kwargs):

        self._modes = None
        self._name = None
        self._f = None
        self.obj = self._builder(*args, **kwargs)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def next(self):
        return next(self.obj)

    def __next__(self):
        return self.next()

    def _builder(self, f, mode='r', reader=None, writer=None, modes=None, name=None, **kwargs):

        self._f = f
        self._modes = modes
        self._name = name
        if mode == 'r':
            return reader(f, **kwargs)
        elif mode in ('w', 'a'):
            return writer(f, **kwargs)
        else:
            raise ValueError("Mode `%s' is unsupported for driver %s: %s" % (mode, name, ', '.join(modes)))

    @classmethod
    def from_path(cls, path, mode='r', **kwargs):
        return cls(open(path, mode), mode=mode, **kwargs)

    @property
    def modes(self):
        return self._modes

    @property
    def name(self):
        return self._name

    def write(self, msg, **kwargs):
        return self.obj.write(msg, **kwargs)

    def closed(self):
        if hasattr(self.obj, 'closed'):
            return self.obj.closed()
        else:
            return self.obj is None

    def close(self):
        if hasattr(self.obj, 'close'):
            return self.obj.close()
        else:
            self.obj = None

    def read(self, size):
        return self.obj.read(size)


class NewlineJSON(BaseDriver):

    name = 'newlinejson'
    extensions = ('json', 'nljson')
    modes = ('r', 'w', 'a')

    def __init__(self, f, mode='r', **kwargs):

        BaseDriver.__init__(
            self,
            f=f, mode=mode,
            reader=newlinejson.Reader,
            writer=newlinejson.Writer,
            modes=self.modes,
            name=self.name,
            **kwargs
        )


class MsgPack(BaseDriver):

    name = 'msgpack'
    extensions = ('msg', 'msgpack')
    modes = ('r', 'w', 'a')

    def __init__(self, f, mode='r', **kwargs):

        BaseDriver.__init__(
            self,
            f=f, mode=mode,
            reader=msgpack.Unpacker,
            writer=msgpack.Packer,
            modes=self.modes,
            name=self.name,
            **kwargs
        )

    def write(self, msg, **kwargs):
        return self.obj.write(msg, **kwargs)


class GZIP(BaseDriver):

    name = 'gzip'
    extensions = 'gz',
    modes = ('r', 'w', 'a')
    compression = True

    def __init__(self, f, mode='r', **kwargs):

        def _reader(f, **kwargs):
            return gzip.GzipFile(fileobj=f, mode=mode, **kwargs)

        def _writer(f, **_kwargs):
            return gzip.GzipFile(fileobj=f, mode=mode, **kwargs)

        BaseDriver.__init__(
            self,
            f=f, mode=mode,
            reader=_reader,
            writer=_writer,
            modes=self.modes,
            name=self.name,
            **kwargs
        )
