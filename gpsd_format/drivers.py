#!/usr/bin/env python


from contextlib import contextmanager

import newlinejson
import msgpack
import gzip
try:
    import ujson
    newlinejson.JSON = ujson
except ImportError:
    ujson = None


_COMPRESSION_EXTENSIONS = ['gz', 'xz', 'zip', 'bz2']
for ce in list(_COMPRESSION_EXTENSIONS):
    _COMPRESSION_EXTENSIONS.append(ce.upper())


def get_driver(name):

    """
    Accepts a string and returns the driver class associatedwith that name.
    """

    for d in REGISTERED_DRIVERS:
        if d.name == name:
            return d
    else:
        raise ValueError("Unrecognized driver name: %s" % name)


def get_compression(name):

    """
    Accepts a string and returns the driver class associated with that name.
    """

    for d in COMPRESSION_DRIVERS:
        if d.name == name:
            return d
    else:
        raise ValueError("Unrecognized compression name: %s" % name)


def detect_file_type(path):

    """
    Given an file path, attempt to determine appropriate driver.
    """

    for ext in path.split('.')[-2:]:
        if ext not in _COMPRESSION_EXTENSIONS:
            return get_driver(ext)


def detect_compression_type(path):

    """
    Given a file path, attempt to determine the appropriate compression driver.
    """

    ext = path.split('.')[-1]
    if ext in _COMPRESSION_EXTENSIONS:
        return get_compression(ext)


class BaseDriver(object):

    def __init__(self, *args, **kwargs):

        self.obj = self.builder(*args, **kwargs)
        self._modes = None
        self._name = None
        self._f = None

    def __iter__(self):
        return self

    def builder(self, f, mode='r', reader=None, writer=None, modes=None, name=None, **kwargs):

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
    @contextmanager
    def from_path(cls, path, mode='r', **kwargs):
        f = open(path, mode)
        c = cls(f, mode=mode, **kwargs)
        try:
            yield c
        finally:
            c.close()
            f.close()

    @property
    def modes(self):
        return self._modes

    @property
    def name(self):
        return self._name

    def next(self):
        return next(self._f)

    def __next__(self):
        return self.next()

    def write(self, msg, **kwargs):
        if self.closed():
            raise IOError("Cannot write to a closed datasource")
        return self.write(msg, **kwargs)

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


class NewlineJSON(BaseDriver):

    def __init__(self, f, mode='r', **kwargs):

        BaseDriver.__init__(
            self,
            f=f, mode=mode,
            reader=newlinejson.Reader,
            writer=newlinejson.Writer,
            modes=('r', 'w', 'a'),
            name='newlinejson',
            **kwargs
        )


class MsgPack(BaseDriver):

    def __init__(self, f, mode='r', **kwargs):

        BaseDriver.__init__(
            self,
            f=f, mode=mode,
            reader=msgpack.Unpacker,
            writer=msgpack.Packer,
            modes=('r', 'w', 'a'),
            name='newlinejson',
            **kwargs
        )

    def write(self, msg, **kwargs):
        return self.obj.write(msg, **kwargs)


# TODO: Make this a function that does some baseline driver validation and logs when a driver can't be registered
# Keep track of every driver that is registered, just the ones the normal API cares about, and just the ones that are
# used for compression IO.
ALL_REGISTERED_DRIVERS = BaseDriver.__subclasses__()
REGISTERED_DRIVERS = [d for d in ALL_REGISTERED_DRIVERS if not getattr(d, 'register', True)]
COMPRESSION_DRIVERS = [d for d in ALL_REGISTERED_DRIVERS if not getattr(d, 'compression', False)]
