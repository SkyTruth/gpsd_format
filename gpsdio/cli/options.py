"""
Commonly used options.  CLI plugin developers are encouraged to use these when
appropriate to keep the gpsdio CLI ecosystem consistent and clean.
"""


import click
import str2type.ext

import gpsdio.drivers


input_driver = click.option(
    '--i-drv', 'input_driver', metavar='NAME', default=None,
    help='Specify the input driver.  Normally auto-detected from file path.',
    type=click.Choice(list(gpsdio.drivers._BaseDriver.by_name.keys()))
)
output_driver = click.option(
    '--o-drv', 'output_driver', metavar='NAME', default=None,
    help='Specify the output driver.  Normally auto-detected from file path.',
    type=click.Choice(list(gpsdio.drivers._BaseDriver.by_name.keys()))
)
input_compression = click.option(
    '--i-cmp', 'input_compression', metavar='NAME', default=None,
    help='Input compression format.  Normally auto-detected from file path.',
    type=click.Choice(list(gpsdio.drivers._BaseCompressionDriver.by_name.keys()))
)
output_compression = click.option(
    '--o-cmp', 'output_compression', metavar='NAME', default=None,
    help='Output compression format.  Normally auto-detected from file path.',
    type=click.Choice(list(gpsdio.drivers._BaseCompressionDriver.by_name.keys()))
)
input_driver_opts = click.option(
    '--ido', 'input_driver_opts', metavar='NAME=VAL', multiple=True,
    callback=str2type.ext.click_cb_key_val,
    help='Input driver options.  JSON values are automatically decoded.',
)
input_compression_opts = click.option(
    '--ico', 'input_compression_opts', metavar='NAME=VAL', multiple=True,
    callback=str2type.ext.click_cb_key_val,
    help='Input compression driver options.  JSON values are automatically decoded.',
)
output_driver_opts = click.option(
    '--odo', 'output_driver_opts', metavar='NAME=VAL', multiple=True,
    callback=str2type.ext.click_cb_key_val,
    help='Output driver options.  JSON values are automatically decoded.',
)
output_compression_opts = click.option(
    '--oco', 'output_compression_opts', metavar='NAME=VAL', multiple=True,
    callback=str2type.ext.click_cb_key_val,
    help='Output compression driver options.  JSON values are automatically decoded.',
)


def _cb_indent(ctx, param, value):

    """
    Click callback for `gpsdio info`'s `--indent` option to let `None` be a
    valid value so the user can disable indentation.
    """

    if value.lower().strip() == 'none':
        return None
    else:
        try:
            return int(value)
        except ValueError:
            raise click.BadParameter("Must be `None` or an integer.")


indent_opt = click.option(
    '--indent', metavar='INTEGER', default='4', callback=_cb_indent,
    help="Indent and pretty print output.  Use `None` to turn off indentation and make output "
         "serializable JSON. (default: 4)"
)
