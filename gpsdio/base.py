"""
Base objects to avoid import errors.
"""


import logging

import six


logger = logging.getLogger('gpsdio')


class _RegisterDriver(type):

    """
    Keep track of drivers, their names, and extensions for easy retrieval later.
    """

    def __init__(driver, name, bases, members):
        # TODO: Add validation.  What methods are required?
        """
        Register drivers by name in one dictionary and by extension in another.
        """

        type.__init__(driver, name, bases, members)
        if members.get('register', True):
            driver.by_name[driver.driver_name] = driver
            for ext in driver.extensions:
                driver.by_extension[ext] = driver


class BaseDriver(six.with_metaclass(_RegisterDriver, object)):

    """
    Provides driver registration and the baseline methods required for driver
    operation.  All other non-compression drivers must subclass this class if
    they want to be registered.  Compression drivers should subclass
    `BaseCompressionDriver()`.


    Creating a driver
    -----------------

    Generally speaking drivers behave just like an instance of `file`, except
    they operate on data stored in a very specific way.  Drivers must handle file
    opening, closing, reading, and writing, via an `open()` method that accepts a
    file path, connection string, etc., `mode`, and `**kwargs` and returns an
    open file-like object.

    Additional critical methods are `__iter__()` and `write()`.  The former must
    yield one dictionary per iteration and the latter must accept a dictionary
    and write it to disk.

    See the `NewlineJSON()` driver for an example of a really simple driver
    and the `MsgPack()` driver for one that is more complex.
    """

    by_name = {}
    by_extension = {}
    register = False
    io_modes = ('r', 'w', 'a')

    def __init__(self, path, mode='r', **kwargs):

        """
        Creates an object that transparently interacts with all supported drivers
        by calling `stream`'s methods.

        Parameters
        ----------
        stream : <object>
            An object provided by a driver that behaves like `file`.
        """

        if mode not in self.io_modes:
            raise ValueError("Mode '{mode}' is unsupported: {io_modes}"
                             .format(mode=mode, io_modes=str(self.io_modes)))
        self._stream = self.open(path, mode=mode, **kwargs)
        self._mode = mode

    def __repr__(self):
        return "<%s driver %s, mode '%s'>" % (
            'closed' if self.closed else 'open',
            self.driver_name,
            self.mode
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return iter(self._stream)

    def open(self, name, mode='r'):

        """
        Open a file, datasource, stream, etc. for reading or writing.
        """

        # This is implemented on the BaseDriver to prevent a recursion error
        # when instantiating the BaseDRiver directly.

        raise NotImplementedError("Drivers must implement this method.")


    @property
    def mode(self):
        return self._mode

    @property
    def closed(self):
        return self._stream.closed

    def __next__(self):

        """
        Get the next message from the underlying stream.
        """

        # Explicitly defined so that __iter__ can work properly.  Otherwise
        # __getattr__ gets in the way.

        return next(self._stream)

    next = __next__

    def write(self, msg):

        """
        Write a message.
        """

        self._stream.write(msg)

    def close(self):

        """
        Close the underlying stream.
        """

        # Explicitly defined otherwise __getattr__ gets in the way.

        self._stream.close()

    def __getattr__(self, item):

        """
        For all other methods, just get it from the underlying `_stream`.
        """

        return getattr(self._stream, item)


class BaseCompressionDriver(BaseDriver):

    """
    A slightly modified subclass of `BaseDriver()` to allow separation of normal
    drivers and compression drivers.
    """

    by_name = {}
    by_extension = {}
    register = False

    def open(self, name, mode='r'):

        """
        Compression drivers must implement this method.  See `BaseDriver()`.
        """

        raise NotImplementedError
