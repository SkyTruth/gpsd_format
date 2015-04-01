"""
Unittests for: gpsd_format._schema_def
"""


import datetime

import six

import gpsd_format.schema
import gpsd_format._schema_def


def test_datetime2str():

    now = datetime.datetime.now()

    # Manually export datetime to the expected string format
    expected_string = now.strftime(gpsd_format.schema.DATETIME_FORMAT)

    # Convert the datetime object to a string with the function
    converted = gpsd_format._schema_def.datetime2str(now)
    assert expected_string == converted

    # Reload the string with datetime and make sure everything matches
    reloaded = datetime.datetime.strptime(converted, gpsd_format.schema.DATETIME_FORMAT)
    assert now.year == reloaded.year
    assert now.month == reloaded.month
    assert now.day == reloaded.day
    assert now.hour == reloaded.hour
    assert now.second == reloaded.second
    assert now.microsecond == reloaded.microsecond
    assert now.tzinfo == reloaded.tzinfo


def test_str2datetime():

    string = '2014-12-19T15:29:36.479005Z'
    expected = datetime.datetime.strptime(string, gpsd_format.schema.DATETIME_FORMAT)
    converted = gpsd_format._schema_def.str2datetime(string)
    assert string == converted.strftime(gpsd_format.schema.DATETIME_FORMAT)
    assert expected.year == converted.year
    assert expected.month == converted.month
    assert expected.day == converted.day
    assert expected.hour == converted.hour
    assert expected.second == converted.second
    assert expected.tzinfo == converted.tzinfo


def test_fields_by_msg_type():
    for msg_type, fields in six.iteritems(gpsd_format._schema_def.fields_by_msg_type):
        for version, definition in six.iteritems(gpsd_format._schema_def.VERSIONS):
            for f in fields:
                assert f in definition


def test_schema_definitions():
    expected_attributes = {
        'default': None,
        'type': None,
        # TODO: Populate schema and uncomment to test and remove the section where good, bad, and test are deleted from the attributes
        # 'good': None,
        # 'bad': None,
        'import': lambda x: x is None or hasattr(x, '__call__'),
        'export': lambda x: x is None or hasattr(x, '__call__'),
        'units': lambda x: isinstance(x, str),
        'description': lambda x: isinstance(x, str),
        'required': lambda x: x in (True, False),
    }
    for version, definition in six.iteritems(gpsd_format._schema_def.VERSIONS.copy()):
        for field, attributes in six.iteritems(definition):
            for _f in ('good', 'bad', 'test'):
                if _f in attributes:
                    del attributes[_f]
            assert sorted(expected_attributes.keys()) == sorted(attributes.keys()), \
                "Schema v%s field `%s' - expected and actual definition fields don't match: %s != %s" \
                % (version, field, sorted(expected_attributes.keys()), sorted(attributes.keys()))

            for attr, test_func in six.iteritems(expected_attributes):
                if test_func is not None:
                    assert test_func(attributes[attr]), "Schema v%s - definition field %s failed its test" \
                                                        % (version, field)
