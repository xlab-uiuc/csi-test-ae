from enum import Enum
import json
import argparse
import os
import random


class Type:
    def __init__(self, name, args):
        self.name = name
        self.args = args

    def get_argstr(self):
        str_args = [str(x) for x in self.args]
        return "(" + ",".join(str_args) + ")" if len(self.args) else ""


class Kind(Enum):
    EXPRESSION = 1
    EXCEPTION = 2


# TODO do exception checking
class Value:
    def __init__(self, kind, value):
        self.kind = kind
        self.value = value


def expr(value):
    return Value(Kind.EXPRESSION, value)


def exn(value):
    return Value(Kind.EXCEPTION, value)


class ExpectedValues:
    def __init__(self, v1, v2=None, v3=None):
        if v2 is None:
            v2 = v1
        if v3 is None:
            v3 = v1
        self.values = {Interface.SQL: v1, Interface.DF: v2, Interface.HQL: v3}


class SystemUnderTest(Enum):
    Spark = 1
    Hive = 2


class Interface(Enum):
    SQL = 1
    DF = 2
    HQL = 3


system_under_test_to_interfaces = {
    SystemUnderTest.Spark: [Interface.SQL, Interface.DF],
    SystemUnderTest.Hive: [Interface.HQL]
}


def get_val(gen, interface):
    val_list = [operator_str(x, interface) for x in gen[0]]
    # dataframes for timestamp (not time) / Date (uppercase) with intervals
    if 'toDF("timestamp")' in val_list[0] or 'toDF("Date")' in val_list[0]:
        return val_list
    else:
        return " ".join(val_list)


def get_expected(gen):
    return gen[1]


def get_expected_val(gen, interface):
    return get_expected(gen).values[interface].value


# TODO can refactor to combine this and make_type functions
def operator_str(op, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return {
            'pi': "pi()",
            'e': "e()",
            'float infinity': "float('infinity')",
            'float inf': "float('inf')",
            'float -infinity': "float('-infinity')",
            'float -inf': "float('-inf')",
            'float NaN': "float('NaN')",
            'double infinity': "double('Infinity')",
            'double inf': "double('Infinity')",
            'double -infinity': "double('-Infinity')",
            'double -inf': "double('-Infinity')",
            'double NaN': "double('NaN')",
            'time': 'cast(',
            'date': 'cast('
        }.get(op, op)
    if interface == Interface.DF:
        return {
            'pi': "math.Pi",
            'e': "math.E",
            'float infinity': "Float.PositiveInfinity",
            'float inf': "Float.PositiveInfinity",
            'float -infinity': "Float.NegativeInfinity",
            'float -inf': "Float.NegativeInfinity",
            'float NaN': "Float.NaN",
            'double infinity': "Double.PositiveInfinity",
            'double inf': "Double.PositiveInfinity",
            'double -infinity': "Double.NegativeInfinity",
            'double -inf': "Double.NegativeInfinity",
            'double NaN': "Double.NaN",
            'as timestamp': "",
            'as date': "",
            'time': "to_timestamp(",
            'date': "to_date("
        }.get(op, op)
    return op


def _make_byte(x, interface):
    if interface == Interface.DF:
        return ['("'+x+'").toByte']
    elif interface == Interface.HQL:
        return [x+'Y']
    return ["cast("+x+" as byte)"]


def _make_short(x, interface):
    if interface == Interface.DF:
        return ['("'+x+'").toShort']
    elif interface == Interface.HQL:
        return [x+'S']
    return ["cast("+x+" as short)"]


def _make_int(x, interface):
    if interface == Interface.DF:
        return ['("'+x+'").toInt']
    return ["cast("+x+" as int)"]


def _make_long(x, interface):
    if interface == Interface.DF:
        return ['BigInt("'+x+'").toLong']
    elif interface == Interface.HQL:
        return [x+'L']
    return ["cast("+x+" as long)"]


def _make_float(x, interface):
    w = x.copy()
    if interface == Interface.DF:
        w.insert(0, "(")
        w.append(").floatValue()")
        return w
    w.insert(0, "cast (")
    w.append(" as float)")
    return w


def gen_valid_byte(interface): # TINYINT in hive
    # min, max, values that tend to have semantic value
    return [(_make_byte("-128", interface), ExpectedValues(expr("-128"), v3=exn(""))),  # ？ Hive does not actually support -128
            (_make_byte("-1", interface), ExpectedValues(expr("-1"))),
            (_make_byte("0", interface), ExpectedValues(expr("0"))),
            (_make_byte("1", interface), ExpectedValues(expr("1"))),
            (_make_byte("127", interface), ExpectedValues(expr("127")))]


def gen_valid_short(interface): # SMALLINT
    # min, max, values that tend to have semantic value
    return [(_make_short("-32768", interface), ExpectedValues(expr("-32768"), v3=exn(""))), # ？ Hive does not actually support -32768
            (_make_short("-1", interface), ExpectedValues(expr("-1"))),
            (_make_short("0", interface), ExpectedValues(expr("0"))),
            (_make_short("1", interface), ExpectedValues(expr("1"))),
            (_make_short("32767", interface), ExpectedValues(expr("32767")))]


def gen_valid_int(interface):
    # min, max, values that tend to have semantic value
    return [(["-2147483648"], ExpectedValues(expr("-2147483648"))),
            (["-1"], ExpectedValues(expr("-1"))),
            (["0"], ExpectedValues(expr("0"))),
            (["1"], ExpectedValues(expr("1"))),
            (["2147483647"], ExpectedValues(expr("2147483647")))]


def gen_valid_long(interface): # BIGINT
    # min, max, values that tend to have semantic value

    return [(_make_long("-9223372036854775808", interface), ExpectedValues(expr("-9223372036854775808"), v3=exn(""))), # ? Hive does not actually support 
            (_make_long("-1", interface), ExpectedValues(expr("-1"))),
            (_make_long("0", interface), ExpectedValues(expr("0"))),
            (_make_long("1", interface), ExpectedValues(expr("1"))),
            (_make_long("9223372036854775807", interface), ExpectedValues(expr("9223372036854775807")))
            ]


def gen_invalid_int(interface):
    # div by 0 not a number
    return [(["0/0"], ExpectedValues(exn(""), v3=exn(""))),
            (["1.1"], ExpectedValues(exn("1.1"), v3=exn(""))),
            ([""], ExpectedValues(exn(""), v3=exn(""))),
            (["9223372036854775808"], ExpectedValues(exn(""), v3=exn(""))),
            (["-9223372036854775809"], ExpectedValues(exn(""), v3=exn(""))),
            (["foo"], ExpectedValues(exn(""), v3=exn("")))]


def gen_invalid_byte(interface):
    return [(_make_byte("0/0", interface), ExpectedValues(exn(""), v3=exn(""))),
            (["2147483647"], ExpectedValues(exn(""), v3=exn(""))),
            (["-2147483647"], ExpectedValues(exn(""), v3=exn(""))),
            (_make_byte("1.1", interface), ExpectedValues(exn("1.1"), v3=exn("")))]


def gen_invalid_short(interface):
    return [(_make_short("0/0", interface), ExpectedValues(exn(""), v3=exn(""))),
            (["2147483647"], ExpectedValues(exn(""), v3=exn(""))),
            (["-2147483647"], ExpectedValues(exn(""), v3=exn(""))),
            (_make_short("1.1", interface), ExpectedValues(exn(""), v3=exn("")))]


def gen_invalid_long(interface):
    return [(_make_long("0/0", interface), ExpectedValues(exn(""), v3=exn(""))),
            (_make_long("1.1", interface), ExpectedValues(exn("1.1"), v3=exn(""))),
            (["9223372036854775808"], ExpectedValues(exn(""), v3=exn(""))),
            (["-9223372036854775809"], ExpectedValues(exn(""), v3=exn("")))]


def gen_valid_float(interface):
    # close to underflow before precision loss, after precision loss, underflow
    # close to overflow, overflow, inf, -inf, nan

    return [(_make_float(["1e-35 * ", "pi"], interface), ExpectedValues(expr("3.1415927E-35"))),
            (_make_float(["1e-40 * ", "pi"], interface), ExpectedValues(expr("3.1416E-40"))),
            (_make_float(["1e-50 * ", "pi"], interface), ExpectedValues(expr("0.0"))),
            (_make_float(["1e35 * ", "e"], interface), ExpectedValues(expr("2.7182818E35"))),
            (_make_float(["1e39 * ", "e"], interface), ExpectedValues(expr("Infinity"))),
            (["float infinity"], ExpectedValues(expr("Infinity"), v3=exn(""))),
            (["float inf"], ExpectedValues(expr("Infinity"), v3=exn(""))),
            (["float -infinity"], ExpectedValues(expr("-Infinity"), v3=exn(""))),
            (["float -inf"], ExpectedValues(expr("-Infinity"), v3=exn(""))),
            (["float NaN"], ExpectedValues(expr("NaN"))),
            (_make_float(["1.0/0"], interface), ExpectedValues(expr("Infinity"), v3=exn(""))),
            (_make_float(["-1.0/0"], interface), ExpectedValues(expr("-Infinity"), v3=exn(""))),
            (_make_float(["0.0/0"], interface), ExpectedValues(expr("NaN"), v3=exn("")))]


def gen_valid_double(interface):
    # close to underflow before precision loss, after precision loss, underflow,
    # close to overflow, overflow, inf, -inf, nan
    #
    # 3.141592653589793E-305, 3.142E-320, 3.0E-323, 2.718284590455E307, Infinity
    # 1e-324 * pi() is an error in DataFrame but 0.0 in SparkSQL,
    # overflow gives inf in DataFrame
    return [(["1e-305 * ", "pi"], ExpectedValues(expr("3.141592653589793E-305"))),
            (["1e-320 * ", "pi"], ExpectedValues(expr("3.142E-320"))),
            (["1e-323 * ", "pi"], ExpectedValues(expr("3.0E-323"))),
            (["1e307 * ", "e"], ExpectedValues(expr("2.718281828459045E307"))),
            (["1e308 * ", "e"], ExpectedValues(expr("Infinity"))),
            (["double infinity"], ExpectedValues(expr("Infinity"))),
            (["double inf"], ExpectedValues(expr("Infinity"))),
            (["double -infinity"], ExpectedValues(expr("-Infinity"))),
            (["double -inf"], ExpectedValues(expr("-Infinity"))),
            (["double NaN"], ExpectedValues(expr("NaN"))),
            (["1.0/0"], ExpectedValues(expr("Infinity"), v3=exn(""))),
            (["-1.0/0"], ExpectedValues(expr("-Infinity"), v3=exn(""))),
            (["0.0/0"], ExpectedValues(expr("NaN"), v3=exn("")))]


def gen_invalid_double(interface):
    return [([""], ExpectedValues(exn(""))),
            (["foo"], ExpectedValues(exn("")))]


def _make_decimal(x, precision, scale, interface):
    if interface == Interface.DF:
        return 'BigDecimal("'+x+'")'
    return x
    # return "cast ('{0}' as decimal({1}, {2}))".format(x, precision, scale)


def gen_valid_decimal(precision, scale, interface):
    # TODO checking for decimal, requires oracle due to rounding issues
    # TODO change pi() to be some constant with at least as much precision and scale as generated
    # precision >= scale
    # last one exhibits that all digits of precision and scale can be used
    # scale digits after decimal

    if precision == 1 and scale == 0:
        return [([_make_decimal("0.", precision, scale, interface)], ExpectedValues(expr("0."))),
                ([_make_decimal("8.e0", precision, scale, interface)], ExpectedValues(expr("8.")))]

    return [([_make_decimal("{0}.{1}".format("3"*(precision-scale-1), "2"*scale), precision, scale, interface)],
             ExpectedValues(expr("{0}.{1}".format("3"*(precision-scale-1), "2"*scale)))),
            ([_make_decimal("8.{0}e{1}".format("8"*(precision-1), precision-scale-1), precision, scale, interface)],
             ExpectedValues(expr("{0}.{1}".format("8"*(precision-scale), "8"*scale))))]


def gen_invalid_decimal(precision, scale, interface):
    return [([_make_decimal("{0}.{1}".format("3"*(precision-scale+1), "2"*scale), precision, scale, interface)],
             ExpectedValues(exn(""))),
            ([_make_decimal("1.0/0", precision, scale, interface)], ExpectedValues(exn("")))]


def gen_valid_string(interface):
    # TODO support strings with \n
    # strings that can be casted to other types
    # strings that can be casted and have semantic meaning,
    # strings with special characters
    return [(['"12831273.24"'], ExpectedValues(expr("12831273.24"))),
            (['"1969-12-31 23:59:59 UTC"'], ExpectedValues(expr("1969-12-31 23:59:59 UTC"))),
            (['"yyyy-MM-dd HH:mm:ss z"'], ExpectedValues(expr("yyyy-MM-dd HH:mm:ss z"))),
            (['"-1"'], ExpectedValues(expr("-1"))),
            (['"0"'], ExpectedValues(expr("0"))),
            (['"1"'], ExpectedValues(expr("1"))),
            (['""'], ExpectedValues(expr(""))),
            ([r'"^fo\\o$"'], ExpectedValues(expr(r"^fo\o$"))),
            (['"www.apache.org"'], ExpectedValues(expr("www.apache.org"))),
            (['"www|apache|org"'], ExpectedValues(expr("www|apache|org"))),
            (['"世界"'], ExpectedValues(expr("世界")))]


def gen_valid_varchar(size, interface):
    def gen2varchar(gen):
        val = get_val(gen, interface)
        return '"' + val[1:min(len(val) - 1, size + 1)] + '"'

    keys = list(map(gen2varchar, gen_valid_string(interface)))
    ret = [([k], ExpectedValues(expr(k[1:-1]))) for k in keys]
    ret.append((['"{0}"'.format('a'*size)], ExpectedValues(expr('a'*size))))
    return ret


def gen_valid_char(size, interface):
    m = gen_valid_varchar(size, interface)
    return [([get_val(gen, interface)],
             ExpectedValues(expr(get_expected_val(gen, interface) +
                                 " " * (size - len(get_expected_val(gen, interface)))))) for gen in m]


def gen_invalid_string(interface):
    return [(['"'], ExpectedValues(exn("")))]


def _make_char(x, size, interface):
    if interface == Interface.DF:
        return [x]
    return ["cast("+x+" as char({0}))".format(size)]


def _make_varchar(x, size, interface):
    if interface == Interface.DF:
        return [x]
    return ["cast("+x+" as varchar({0}))".format(size)]


def gen_invalid_char(size, interface):
    return [
            (['"'], ExpectedValues(exn(""))),
            (_make_char('"{0}"'.format('b' * (size + 1)), size, interface), ExpectedValues(exn("")))
    ]


def gen_invalid_varchar(size, interface):
    return [
            (['"'], ExpectedValues(exn(""))),
            (_make_varchar('"{0}"'.format('b' * (size + 1)), size, interface), ExpectedValues(exn("")))
    ]


def gen_valid_binary(interface):
    def get_binary(n):
        return n.to_bytes((n.bit_length() + 7) // 8, 'big', signed=True) or b'\0'

    def make_binary(x):
        if interface == Interface.SQL:
            return ["X'" + str(get_binary(int(x))).replace("\\x", "").replace("b'", "")]
        if interface == Interface.HQL:
            return ['"' + str(get_binary(int(x))).replace("\\x", "").replace("b'", "").replace("'", "") + '"']
        if interface == Interface.DF:
            return ['BigInt("'+x+'").toByteArray']

    valid_bin = ["-2147483648", "-1", "0", "1", "2147483647"]

    return [(make_binary(k), ExpectedValues(expr(None))) for k in valid_bin]


def gen_invalid_binary(interface):
   return [(["'spark'"], ExpectedValues(exn(""))),
            (["X'spark'"], ExpectedValues(exn(""))),
            (["25"], ExpectedValues(exn(""))),
            (["1.1"], ExpectedValues(exn(""))),
            (["00FF"], ExpectedValues(exn(""))),
            ([""], ExpectedValues(exn(""))),
            (["b''"], ExpectedValues(exn("")))]


def gen_valid_boolean(interface):
    return [(['true'], ExpectedValues(expr("true"))),
            (['false'], ExpectedValues(expr("false")))]


def gen_invalid_boolean(interface):
    return [(['tf'], ExpectedValues(exn(""))),
            ([""], ExpectedValues(exn(""))),
            (["1"], ExpectedValues(exn(""))),
            (["foo"], ExpectedValues(exn(""))),
            (["1.1"], ExpectedValues(exn("")))]


def _make_timestamp(time, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return ['cast("', time, '"as timestamp)']
    if interface == Interface.DF:
        return ['Seq("', time, '").toDF("time").select(to_timestamp(col("time")).as("to_timestamp"))'
                '.first().getAs[java.sql.Timestamp](0)']
    return None


def gen_valid_timestamp(interface):
    return [(_make_timestamp("2022", interface), ExpectedValues(expr("2022-01-01 00:00:00"), v3=exn(""))),
            (_make_timestamp("2022-01", interface), ExpectedValues(expr("2022-01-01 00:00:00"), v3=exn(""))),
            (_make_timestamp("2022-01-05", interface), ExpectedValues(expr("2022-01-05 00:00:00"))),
            (_make_timestamp("1969-12-31 23:59:59 UTC", interface), ExpectedValues(expr("1969-12-31 17:59:59"))),
            (_make_timestamp("2021-05-27T03:20:50.28 Z", interface), ExpectedValues(expr("2021-05-26 22:20:50.28"), v3=exn(""))),
            (_make_timestamp("2021-08-18 00:20:50.28 -08", interface), ExpectedValues(expr("2021-08-18 03:20:50.28"))),
            (_make_timestamp("2016-02-29 03:20:50.28 +08", interface), ExpectedValues(expr("2016-02-28 13:20:50.28"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-01 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-01 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-03 04:03:02"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-01 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-01 00:00:03"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-04 11:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-02 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2021-12-31 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-01 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-01 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-03 04:03:02"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-01 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-01 00:00:03"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-04 11:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-02 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2021-12-31 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-05 00:00:00"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-05 00:00:00"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-07 04:03:02"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-05 00:00:00"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-05 00:00:03"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-08 11:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01-05", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-06 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("2022-01-05", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-04 00:00:00"), v3=exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("1970-12-31 17:59:59"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("1970-12-31 17:59:59"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("1970-01-02 22:03:01"), v3=exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("1971-02-28 17:59:59"), v3=exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("1969-12-31 18:00:02"), v3=exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("1970-05-04 04:59:59"), v3=exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "+INTERVAL '1 day'", interface), ExpectedValues(expr("1970-01-01 17:59:59"), v3=exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "-INTERVAL '1 day'", interface), ExpectedValues(expr("1969-12-30 17:59:59"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2022-05-26 22:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2022-05-26 22:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2021-05-29 02:23:52.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2022-07-26 22:20:50.28"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2021-05-26 22:20:53.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2021-09-27 09:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2021-05-27 22:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2021-05-25 22:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2022-08-18 03:20:50.28"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2022-08-18 03:20:50.28"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2021-08-20 07:23:52.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2022-10-18 03:20:50.28"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2021-08-18 03:20:53.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2021-12-19 14:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2021-08-19 03:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2021-08-17 03:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2017-02-28 13:20:50.28"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2017-02-28 13:20:50.28"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2016-03-01 17:23:52.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2017-04-28 13:20:50.28"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2016-02-28 13:20:53.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2016-07-01 00:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2016-02-29 13:20:50.28"), v3=exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2016-02-27 13:20:50.28"), v3=exn("")))]


def gen_invalid_timestamp(interface):
    # NULL values
    return [(_make_timestamp("1969-12-31 23:59:59 B", interface), ExpectedValues(exn(""))),
            (_make_timestamp("2021-08-18r03:20:50.28 Z", interface), ExpectedValues(exn(""))),
            (_make_timestamp("k2021-08-18 00:20:50.28 +08", interface), ExpectedValues(exn(""))),
            (_make_timestamp("2016-02-29 03:20:500.28 +08", interface), ExpectedValues(exn(""))),
            (_make_timestamp("2016-02-30 03:20:50.28 AM +08", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "INTERVAL '12' MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "INTERVAL 12 MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "INTERVAL '1-2' year to month", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "INTERVAL '123 11' day to hour", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "+INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 B", "-INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "INTERVAL '12' MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "INTERVAL 12 MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "INTERVAL '1-2' year to month", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "INTERVAL '123 11' day to hour", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "+INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2021-08-18r03:20:50.28 Z", "-INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "INTERVAL '12' MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "INTERVAL 12 MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "INTERVAL '1-2' year to month", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "INTERVAL '123 11' day to hour", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "+INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("k2021-08-18 00:20:50.28 +08", "-INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "INTERVAL '12' MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "INTERVAL 12 MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "INTERVAL '1-2' year to month", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "INTERVAL '123 11' day to hour", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "+INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-29 03:20:500.28 +08", "-INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "INTERVAL '12' MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "INTERVAL 12 MONTH", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "INTERVAL '1-2' year to month", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "INTERVAL '123 11' day to hour", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "+INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_timestamp_with_interval("2016-02-30 03:20:50.28 AM +08", "-INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            # exn values
            (_make_timestamp_with_interval("2022", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2022", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2022", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("2022", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2022-01", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2022-01", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("2022-01", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2022-01-05", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2022-01-05", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("2022-01-05", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("2021-05-27T03:20:50.28 Z", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("2021-08-18 00:20:50.28 -08", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_timestamp_with_interval("2016-02-29 03:20:50.28 +08", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form")))]


def _make_date(date, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return ['cast("', date, '"as date)']
    if interface == Interface.DF:
        return ['Seq("', date, '").toDF("date").select(to_date(col("date")).as("to_date"))'
                '.first().getAs[java.sql.Date](0)']
    return None


def gen_valid_date(interface):
    return [(_make_date("2022", interface), ExpectedValues(expr("2022-01-01"), v3=exn(""))),
            (_make_date("2022-01", interface), ExpectedValues(expr("2022-01-01"), v3=exn(""))),
            (_make_date("2022-01-05", interface), ExpectedValues(expr("2022-01-05"))),
            (_make_date("2022-01-05T", interface), ExpectedValues(expr("2022-01-05"), v3=exn(""))),
            (_make_date("2016-02-29", interface), ExpectedValues(expr("2016-02-29"))),
            (_make_date("1969-12-31 23:59:59 UTC", interface), ExpectedValues(expr("1969-12-31"))),
            (_make_date_with_interval("2022", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-01"), v3=exn(""))),
            (_make_date_with_interval("2022", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-01"), v3=exn(""))),
            (_make_date_with_interval("2022", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-03 04:03:02"), v3=exn(""))),
            (_make_date_with_interval("2022", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-01"), v3=exn(""))),
            (_make_date_with_interval("2022", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-01 00:00:03"), v3=exn(""))),
            (_make_date_with_interval("2022", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-04 11:00:00"), v3=exn(""))),
            (_make_date_with_interval("2022", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-02"), v3=exn(""))),
            (_make_date_with_interval("2022", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2021-12-31"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-01"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-01"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-03 04:03:02"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-01"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-01 00:00:03"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-04 11:00:00"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-02"), v3=exn(""))),
            (_make_date_with_interval("2022-01", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2021-12-31"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-05"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-05"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-07 04:03:02"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-05"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-05 00:00:03"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-08 11:00:00"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-06"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-04"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2023-01-05"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2023-01-05"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2022-01-07 04:03:02"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2023-03-05"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2022-01-05 00:00:03"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2022-05-08 11:00:00"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-06"), v3=exn(""))),
            (_make_date_with_interval("2022-01-05T", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2022-01-04"), v3=exn(""))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("2017-02-28"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("2017-02-28"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("2016-03-02 04:03:02"), v3=exn(""))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("2017-04-29"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("2016-02-29 00:00:03"), v3=exn(""))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("2016-07-01 11:00:00"), v3=exn(""))),
            (_make_date_with_interval("2016-02-29", "+INTERVAL '1 day'", interface), ExpectedValues(expr("2016-03-01"), v3=exn(""))),
            (_make_date_with_interval("2016-02-29", "-INTERVAL '1 day'", interface), ExpectedValues(expr("2016-02-28"), v3=exn(""))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '12' MONTH", interface), ExpectedValues(expr("1970-12-31"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL 12 MONTH", interface), ExpectedValues(expr("1970-12-31"), v3=exn(""))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(expr("1970-01-02 04:03:02"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1-2' year to month", interface), ExpectedValues(expr("1971-02-28"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(expr("1969-12-31 00:00:03"), v3=exn(""))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '123 11' day to hour", interface), ExpectedValues(expr("1970-05-03 11:00:00"), v3=exn(""))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "+INTERVAL '1 day'", interface), ExpectedValues(expr("1970-01-01"), v3=exn(""))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "-INTERVAL '1 day'", interface), ExpectedValues(expr("1969-12-30"), v3=exn("")))]


def gen_invalid_date(interface):
    #
    return [(_make_date("2016-02-30", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "INTERVAL '12' MONTH", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "INTERVAL 12 MONTH", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "INTERVAL '52 hours 3 minutes 2 seconds'", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "INTERVAL '1-2' year to month", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "INTERVAL 1 second 2 seconds", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "INTERVAL '123 11' day to hour", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "+INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2016-02-30", "-INTERVAL '1 day'", interface), ExpectedValues(exn(""))),
            (_make_date_with_interval("2022", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_date_with_interval("2022", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_date_with_interval("2022", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_date_with_interval("2022", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_date_with_interval("2022", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_date_with_interval("2022-01", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_date_with_interval("2022-01", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022-01", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022-01", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_date_with_interval("2022-01", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_date_with_interval("2022-01", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022-01", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022-01", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_date_with_interval("2022-01", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022-01-05", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022-01-05", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_date_with_interval("2022-01-05", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022-01-05T", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2022-01-05T", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_date_with_interval("2022-01-05T", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2016-02-29", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("2016-02-29", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_date_with_interval("2016-02-29", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '10 years -11 month -12 days'", interface), ExpectedValues(exn("Cannot mix year-month and day-time fields"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '2147483647 days 24 hours'", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "extract(epoch from interval '1000000000 days')", interface), ExpectedValues(exn("ArithmeticException"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "extract(epoch from interval '1000000000 microseconds')", interface), ExpectedValues(exn("Literals of type 'epoch' are currently not supported"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1 2:03:04' day to hour", interface), ExpectedValues(exn("Interval string does not match day-time format"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1-2'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '1 day 01:23:45.6789'", interface), ExpectedValues(exn("Cannot parse the INTERVAL value"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '123 11' day", interface), ExpectedValues(exn("invalid unit"))),
            (_make_date_with_interval("1969-12-31 23:59:59 UTC", "INTERVAL '123 days hours' day", interface), ExpectedValues(exn("Can only use numbers in the interval value part for multiple unit value pairs interval form")))]


def _make_timestamp_with_interval(timestamp, interval, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return ['cast("' + timestamp + '"as timestamp) + ' + interval]
    # toDF("timestamp") distinguish from simple timestamp
    if interface == Interface.DF:
        return ['Seq("' + timestamp + '").toDF("timestamp").select(to_timestamp(col("timestamp")).as("to_timestamp"))' + \
            '.first().getAs[java.sql.Timestamp](0)', interval]
    return None


def _make_date_with_interval(date, interval, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return ['cast("' + date + '"as date) + ' + interval]
    # toDF("Date") uppercase to distinguish from simple Date
    if interface == Interface.DF:
        return ['Seq("' + date + '").toDF("Date").select(to_date(col("Date")).as("to_date"))' + \
            '.first().getAs[java.sql.Date](0)', interval]
    return None


def gen_valid_interval(interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return [(['INTERVAL "12" MONTH'], ExpectedValues(expr('1-0'))),
                (['INTERVAL 12 MONTH'], ExpectedValues(expr('1-0'))),
                (['INTERVAL "52 hours 3 minutes 2 seconds"'], ExpectedValues(expr('2 04:03:02:000000000'))),
                (['INTERVAL "1-2" year to month'], ExpectedValues(expr('1-2'))),
                (['INTERVAL 1 second 2 seconds'], ExpectedValues(expr('0 00:00:03:000000000'))),
                (['INTERVAL "123 11" day to hour'], ExpectedValues(expr('123 11:00:00:000000000'))),
                (['+INTERVAL "1 day"'], ExpectedValues(expr('1 00:00:00:000000000'))),
                (['-INTERVAL "1 day"'], ExpectedValues(expr('-1 00:00:00:000000000')))]
    return None


def gen_invalid_interval(interface):
    if interface in [Interface.SQL, Interface.HQL]:
        return [(['INTERVAL "10 years -11 month -12 days"'], ExpectedValues(exn('Cannot mix year-month and day-time fields'))),
                (['INTERVAL "2147483647 days 24 hours"'], ExpectedValues(exn('ArithmeticException'))),
                (['extract(epoch from interval "1000000000 days")'], ExpectedValues(exn('ArithmeticException'))),
                (['extract(epoch from interval "1000000000 microseconds")' ], ExpectedValues(exn('Literals of type "epoch" are currently not supported'))),
                (['INTERVAL "1 2:03:04" day to hour'], ExpectedValues(exn('Interval string does not match day-time format'))),
                (['INTERVAL "1-2"'], ExpectedValues(exn('Cannot parse the INTERVAL value'))),
                (['INTERVAL "1 day 01:23:45.6789"'], ExpectedValues(exn('Cannot parse the INTERVAL value'))),
                (['INTERVAL "123 11" day'], ExpectedValues(exn('invalid unit'))),
                (['INTERVAL "123 days hours" day'], ExpectedValues(exn('Can only use numbers in the interval value part for multiple unit value pairs interval form')))]
    return None


# len(keys) == len(vals)
def expected_map2str(m, key_is_str, val_is_str):
    s = "{"
    for (k, v) in m.items():
        new_k = '"' + k + '"' if key_is_str else k
        new_v = '"' + v + '"' if val_is_str else v
        s += "{0}:{1},".format(new_k, new_v)
    return s[:-1] + "}"


def input_map2str(m, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        s = "map("
        for (k, v) in m.items():
            s += "{0}, {1},".format(k, v)
        return s[:-1] + ")"
    if interface == Interface.DF:
        s = "Map("
        for (k, v) in m.items():
            s += "{0} -> {1},".format(k, v)
        return s[:-1] + ")"
    return None


def expected_array2str(a):
    s = "["
    for v in a:
        s += "{0},".format(v)
    return s[:-1] + "]"


def input_array2str(a, interface):
    if interface in [Interface.SQL, Interface.HQL]:
        s = "array("
        for v in a:
            s += "{0},".format(v)
        return s[:-1] + ")"
    if interface == Interface.DF:
        s = "Array("
        for v in a:
            s += "{0},".format(v)
        return s[:-1] + ")"


def gen_valid_map2(interface):
    # only inserts with single element for now to avoid implementing equivalence checker
    inner_key_options = gen_valid_string(interface)
    inner_val_options = gen_valid_double(interface)
    outer_val_options = []
    outer_val_expected = []
    ret = []
    for i in range(1):
        key_picked = inner_key_options[i % len(inner_key_options)]
        val_picked = inner_val_options[i % len(inner_val_options)]
        inner_map = {}
        inner_expected = {}
        for j in range(1):
            inner_map[get_val(key_picked, interface)] = get_val(val_picked, interface)
            inner_expected[get_expected_val(key_picked, interface)] = get_expected_val(val_picked, interface)
        outer_val_options.append(input_map2str(inner_map, interface))
        outer_val_expected.append(expected_map2str(inner_expected, True, False))
    outer_key_options = gen_valid_int(interface)
    key_picked = outer_key_options[0]
    outer_map = {}
    outer_expected = {}
    for i in range(1):
        k = get_val(key_picked, interface)
        outer_map[k] = outer_val_options[i]
        outer_expected[k] = outer_val_expected[i]
    ret.append(([input_map2str(outer_map, interface)],
                ExpectedValues(expr(expected_map2str(outer_expected, False, False)))))
    return ret


def gen_valid_map1(interface):
    # only inserts with single element for now to avoid implementing equivalence checker
    inner_key_options = gen_valid_string(interface)
    inner_val_options = gen_valid_double(interface)
    outer_val_options = []
    outer_val_expected = []
    ret = []
    for i in range(1):
        key_picked = inner_key_options[i % len(inner_key_options)]
        val_picked = inner_val_options[i % len(inner_val_options)]
        inner_map = {}
        inner_expected = {}
        for j in range(1):
            inner_map[get_val(key_picked, interface)] = get_val(val_picked, interface)
            inner_expected[get_expected_val(key_picked, interface)] = get_expected_val(val_picked, interface)
        outer_val_options.append(input_map2str(inner_map, interface))
        outer_val_expected.append(expected_map2str(inner_expected, True, False))
        ret.append(([input_map2str(inner_map, interface)],
                    ExpectedValues(expr(expected_map2str(inner_expected, True, False)))))
    return ret


def gen_valid_array(interface):
    outer_array = []
    outer_expected = []
    val_options = gen_valid_double(interface)
    for i in range(2):
        picked = val_options[:i+1]
        inner_array = [get_val(v, interface) for v in picked]
        inner_expected = [get_expected_val(v, interface) for v in picked]
        outer_array.append(input_array2str(inner_array, interface))
        outer_expected.append(expected_array2str(inner_expected))
    return [([input_array2str(outer_array, interface)],
             ExpectedValues(expr(expected_array2str(outer_expected))))]


def input_struct2str(m, interface):
    if interface == Interface.HQL:
        s = 'named_struct('
        for (k, v) in m.items():
            s += "'{0}', '{1}',".format(k, v)
        return s[:-1] + ")"
    if interface == Interface.DF:
        s = 'Seq(Row('
        for (k, v) in m.items():
            s += "'{0}',".format(v)
        return s[:-1] + ')'
    return None


def gen_valid_struct(interface):
    f11_name = (['"f11"'], ExpectedValues(expr("f11")))
    f12_name = (['"f12"'], ExpectedValues(expr("f12")))
    f1_name = (['"f1"'], ExpectedValues(expr("f1")))
    f2_name = (['"f2"'], ExpectedValues(expr("f2")))
    f11_options = gen_valid_string(interface)
    f12_options = gen_valid_double(interface)
    f2_options = gen_valid_boolean(interface)

    f11_picked = f11_options[0]
    f12_picked = f12_options[0]
    f2_picked = f2_options[0]

    inner_struct = {}
    inner_expected = {}
    inner_struct[get_val(f11_name, interface)] = get_val(f11_picked, interface)
    inner_expected[get_expected_val(f11_name, interface)] \
                    = get_expected_val(f11_picked, interface)
    inner_struct[get_val(f12_name, interface)] = get_val(f12_picked, interface)
    inner_expected[get_expected_val(f12_name, interface)] \
                    = get_expected_val(f12_picked, interface)

    outer_struct = {}
    outer_expected = {}
    # using expected_map2str as struct is equivalent but key is always string
    outer_struct[get_val(f1_name, interface)] = input_struct2str(inner_struct, interface)
    outer_expected[get_expected_val(f1_name, interface)] \
                    = expected_map2str(inner_expected, True, False)
    outer_struct[get_val(f2_name, interface)] = get_val(f2_picked, interface)
    outer_expected[get_expected_val(f2_name, interface)] \
                    = get_expected_val(f2_picked, interface)

    # print(input_struct2str(outer_struct, interface))
    return [([input_struct2str(outer_struct, interface)],
             ExpectedValues(expr(expected_map2str(outer_expected, True, False))))]


def expected_union2str(tag, vals):
    return '{{{0}:{1}}}'.format(tag, vals[tag])


def input_union2str(tag, vals, interface):
    if interface == Interface.HQL:
        s = 'create_union({0},'.format(tag)
        for v in vals:
            s += "{0},".format(v)
        return s[-1] + ')'
    if interface == Interface.DF:
        return ""
    return None


def gen_valid_union(interface):
    int_options = gen_valid_int(interface)
    map_options = gen_valid_map1(interface)
    array_options = gen_valid_array(interface)
    ts_options = gen_valid_timestamp(interface)

    int_picked = int_options[0]
    map_picked = map_options[0]
    array_picked = array_options[0]
    ts_picked = ts_options[0]

    union = [get_val(int_picked, interface), get_val(map_picked, interface), \
             get_val(array_picked, interface), get_val(ts_picked, interface)]

    union_expected = [get_expected_val(int_picked, interface), \
                      get_expected_val(map_picked, interface), \
                      get_expected_val(array_picked, interface), \
                      get_expected_val(ts_picked, interface)]
    
    tag = 1

    return [([input_union2str(tag, union, interface)],
             ExpectedValues(expr(expected_union2str(tag, union_expected))))]


def gen_empty(interface):
    return []


def sqltype2sparktype(type_obj):
    def get_argstr():
        return str(tuple(type_obj.args)).replace(",)", ")")

    return {
        "BYTE": "ByteType",
        "SHORT": "ShortType",
        "INT": "IntegerType",
        "LONG": "LongType",
        "FLOAT": "FloatType",
        "DOUBLE": "DoubleType",
        "DECIMAL": "DecimalType"+get_argstr(),
        "STRING": "StringType",
        "VARCHAR": "VarcharType"+get_argstr(),
        "CHAR": "CharType"+get_argstr(),
        "BINARY": "BinaryType",
        "BOOLEAN": "BooleanType",
        "TIMESTAMP": "TimestampType",
        "DATE": "DateType",
        "MAP<STRING, DOUBLE>": "MapType(StringType, DoubleType)",
        "MAP<INT, MAP<STRING, DOUBLE>>": "MapType(IntegerType, MapType(StringType, DoubleType))",
        "ARRAY<ARRAY<DOUBLE>>": "ArrayType(ArrayType(DoubleType))"
    }[type_obj.name]


def hqltype2sparktype(type_obj):
    def get_argstr():
        return str(tuple(type_obj.args)).replace(",)", ")")

    return {
        "TINYINT": "ByteType",
        "SMALLINT": "ShortType",
        "INT": "IntegerType",
        "BIGINT": "LongType",
        "FLOAT": "FloatType",
        "DOUBLE": "DoubleType",
        "DECIMAL": "DecimalType"+get_argstr(),
        "STRING": "StringType",
        "VARCHAR": "VarcharType"+get_argstr(),
        "CHAR": "CharType"+get_argstr(),
        "BINARY": "BinaryType",
        "BOOLEAN": "BooleanType",
        "TIMESTAMP": "TimestampType",
        "DATE": "DateType",
        "MAP<STRING, DOUBLE>": "MapType(StringType, DoubleType)",
        "MAP<INT, MAP<STRING, DOUBLE>>": "MapType(IntegerType, MapType(StringType, DoubleType))",
        "ARRAY<ARRAY<DOUBLE>>": "ArrayType(ArrayType(DoubleType))"
    }[type_obj.name]


qltype2sparktype = {
    "spark": sqltype2sparktype,
    "hive": hqltype2sparktype
}


sqltype2valid_gen_fn = {
    "BYTE": gen_valid_byte,
    "SHORT": gen_valid_short,
    "INT": gen_valid_int,
    "LONG": gen_valid_long,
    "FLOAT": gen_valid_float,
    "DOUBLE": gen_valid_double,
    "DECIMAL": gen_valid_decimal,
    "STRING": gen_valid_string,
    "VARCHAR": gen_valid_varchar,
    "CHAR": gen_valid_char,
    "BINARY": gen_valid_binary,
    "BOOLEAN": gen_valid_boolean,
    "TIMESTAMP": gen_valid_timestamp,
    "DATE": gen_valid_date,
    "MAP<STRING, DOUBLE>": gen_valid_map1,
    "MAP<INT, MAP<STRING, DOUBLE>>": gen_valid_map2,
    "ARRAY<ARRAY<DOUBLE>>": gen_valid_array
}

sqltype2invalid_gen_fn = {
    "BYTE": gen_invalid_byte,
    "SHORT": gen_invalid_short,
    "INT": gen_invalid_int,
    "LONG": gen_invalid_long,
    "FLOAT": gen_invalid_double,
    "DOUBLE": gen_invalid_double,
    "DECIMAL": gen_invalid_decimal,
    "STRING": gen_invalid_string,
    "VARCHAR": gen_invalid_varchar,
    "CHAR": gen_invalid_char,
    "BINARY": gen_invalid_binary,
    "BOOLEAN": gen_invalid_boolean,
    "TIMESTAMP": gen_invalid_timestamp,
    "DATE": gen_invalid_date,
    "MAP<STRING, DOUBLE>": gen_empty,
    "MAP<INT, MAP<STRING, DOUBLE>>": gen_empty,
    "ARRAY<ARRAY<DOUBLE>>": gen_empty
}

hqltype2valid_gen_fn = {
    "TINYINT": gen_valid_byte,
    "SMALLINT": gen_valid_short,
    "INT": gen_valid_int,
    "BIGINT": gen_valid_long,
    "FLOAT": gen_valid_float,
    "DOUBLE": gen_valid_double, # also DOUBLE PRECISION
    "DECIMAL": gen_valid_decimal, # also NUMERIC
    "STRING": gen_valid_string,
    "VARCHAR": gen_valid_varchar,
    "CHAR": gen_valid_char,
    "BINARY": gen_valid_binary,
    "BOOLEAN": gen_valid_boolean,
    "TIMESTAMP": gen_valid_timestamp,
    "DATE": gen_valid_date,
    # # TODO "INTERVAL": gen_valid_interval,
    "MAP<STRING, DOUBLE>": gen_valid_map1,
    "MAP<INT, MAP<STRING, DOUBLE>>": gen_valid_map2,
    "ARRAY<ARRAY<DOUBLE>>": gen_valid_array,
    # "STRUCT<f1: STRUCT<f11: STRING, f12: DOUBLE>, f2: BOOLEAN>": gen_valid_struct,
    # "UNIONTYPE<INT, MAP<STRING, DOUBLE>, ARRAY<ARRAY<DOUBLE>>, TIMESTAMP>": gen_valid_union
}

hqltype2invalid_gen_fn = {
    "TINYINT": gen_invalid_byte,
    "SMALLINT": gen_invalid_short,
    "INT": gen_invalid_int,
    "BIGINT": gen_invalid_long,
    "FLOAT": gen_invalid_double,
    "DOUBLE": gen_invalid_double,
    "DECIMAL": gen_invalid_decimal,
    "STRING": gen_invalid_string,
    "VARCHAR": gen_invalid_varchar,
    "CHAR": gen_invalid_char,
    "BINARY": gen_invalid_binary,
    "BOOLEAN": gen_invalid_boolean,
    "TIMESTAMP": gen_invalid_timestamp,
    "DATE": gen_invalid_date,
    "MAP<STRING, DOUBLE>": gen_empty,
    "MAP<INT, MAP<STRING, DOUBLE>>": gen_empty,
    "ARRAY<ARRAY<DOUBLE>>": gen_empty,
    # "STRUCT<f1: STRUCT<f11: STRING, f12: DOUBLE>, f2: BOOLEAN>": gen_empty,
    # "UNIONTYPE<INT, MAP<STRING, DOUBLE>, ARRAY<ARRAY<DOUBLE>>, TIMESTAMP>": gen_empty
}

def interfaces2translation(og_interface, final_interface):
    return {
        Interface.SQL: {Interface.SQL: {x: x for x in sql2hqltypes.keys()},
                        Interface.HQL: sql2hqltypes},
        Interface.HQL: {Interface.SQL: hql2sqltypes}
    }[og_interface][final_interface]

sql2hqltypes = {
    "BYTE": "TINYINT",
    "SHORT": "SMALLINT",
    "INT": "INT",
    "LONG": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL",
    "STRING": "STRING",
    "VARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "BINARY": "BINARY",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "MAP<STRING, DOUBLE>": "MAP<STRING, DOUBLE>",
    "MAP<INT, MAP<STRING, DOUBLE>>": "MAP<INT, MAP<STRING, DOUBLE>>",
    "ARRAY<ARRAY<DOUBLE>>": "ARRAY<ARRAY<DOUBLE>>"
}

hql2sqltypes = {
    "TINYINT": "BYTE",
    "SMALLINT": "SHORT",
    "INT": "INT",
    "BIGINT": "LONG",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL",
    "STRING": "STRING",
    "VARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "BINARY": "BINARY",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "MAP<STRING, DOUBLE>": "MAP<STRING, DOUBLE>",
    "MAP<INT, MAP<STRING, DOUBLE>>": "MAP<INT, MAP<STRING, DOUBLE>>",
    "ARRAY<ARRAY<DOUBLE>>": "ARRAY<ARRAY<DOUBLE>>"
}

interface2valid_gen_fn = {
    Interface.SQL: sqltype2valid_gen_fn,
    Interface.DF: {Interface.SQL: sqltype2valid_gen_fn,
                   Interface.HQL: hqltype2valid_gen_fn},
    Interface.HQL: hqltype2valid_gen_fn
}

interface2invalid_gen_fn = {
    Interface.SQL: sqltype2invalid_gen_fn,
    Interface.DF: {Interface.SQL: sqltype2invalid_gen_fn,
                   Interface.HQL: hqltype2invalid_gen_fn},
    Interface.HQL: hqltype2invalid_gen_fn
}


def get_stats(interface):
    stats = {"total": {"valid": 0, "invalid": 0}}
    for key in interface2valid_gen_fn[interface].keys():
        t = gen_type(key)
        stats[key] = {"valid": len(interface2valid_gen_fn[interface][key](*t.args, interface)),
                      "invalid": len(interface2invalid_gen_fn[interface][key](*t.args, interface))}
    for key in interface2valid_gen_fn[interface].keys():
        stats["total"]["valid"] += stats[key]["valid"]
        stats["total"]["invalid"] += stats[key]["invalid"]
    return stats


def gen_type(t):
    if t == "DECIMAL":
        return Type("DECIMAL", [20, 10])
    if t == "VARCHAR":
        return Type("VARCHAR", [10])
    if t == "CHAR":
        return Type("CHAR", [5])
    return Type(t, [])


def expand_types(type_names):
    expanded_types = []
    for t in type_names:
        expanded_types.append(gen_type(t))
    return expanded_types


def gen_entries(ql_interface, dry_run=False):
    types = list(interface2valid_gen_fn[ql_interface].keys())
    if dry_run:
        # Choose a random type, since we do not need to test for all inputs for a dry run.
        types = [random.choice(types)]
    type_list = expand_types(types)
    type_objs = []
    entries = []
    expected_entries = []
    row_idx = 0
    for i in range(len(type_list)):
        ql_gen = interface2valid_gen_fn[ql_interface][type_list[i].name](*type_list[i].args, ql_interface) + \
            interface2invalid_gen_fn[ql_interface][type_list[i].name](*type_list[i].args, ql_interface)
        df_gen = interface2valid_gen_fn[Interface.DF][ql_interface][type_list[i].name](*type_list[i].args, Interface.DF) + \
            interface2invalid_gen_fn[Interface.DF][ql_interface][type_list[i].name](*type_list[i].args, Interface.DF)
        for j in range(len(ql_gen)):
            type_objs.append([gen_type("INT"), type_list[i]])
            row = [pack_input(ql_interface, str(row_idx), str(row_idx)),
                pack_input(ql_interface, get_val(ql_gen[j], ql_interface),
                        get_val(df_gen[j], Interface.DF))]
            expected_row = [ExpectedValues(expr(str(row_idx))),
                            get_expected(ql_gen[j])]
            entries.append(row)
            expected_entries.append(expected_row)
            row_idx += 1
    return type_objs, entries, expected_entries


def get_rt(exp):
    if exp.kind == Kind.EXPRESSION:
        if exp.value is None:
            return "None"
        else:
            return exp.value
    else:
        return ""


def write_original(cols, tables, expected_values, wql_interface):
    num_tables = len(tables)
    original_dict = dict()
    for i in range(num_tables):
        if all (e.name != 'INTERVAL' for e in cols[wql_interface][i]):
            original_dict[i] = {
                "value": tables[i][1],
                "type": cols[wql_interface][i][1].name + cols[wql_interface][i][1].get_argstr(),
                "valid": True if not expected_values[i][1].kind == Kind.EXCEPTION else False
            }
    with open(os.path.join(args.log_dir, 't_original.json'), 'w') as wf:
        json.dump(original_dict, wf, indent=4)


def write_ql(cols, tables_per_ifc, expected_values_per_ifc, format_type, interoperability_test):
    ql_interfaces = system_interface.values()
    num_tables = len(tables_per_ifc[wql_interface])
    primary_write_interface_tables = list()
    with open(os.path.join(args.log_dir, 'w_'+wql_interface.name.lower()+"_"+format_type), 'w') as wf:
        # This extra classpath is required when Spark is used as Hive's execution engine.
        if wql_interface == Interface.HQL:
            hive_exec_jar_path = os.path.join(os.environ.get('HIVE_HOME'), 'lib', 'hive-exec-3.1.2.jar')
            wf.write("set spark.driver.extraClassPath=%s;\n" % hive_exec_jar_path)
            wf.write("set spark.executor.extraClassPath=%s;\n" % hive_exec_jar_path)
        for i, table_values in enumerate(tables_per_ifc[wql_interface]):
            table_name = "ws" + str(i)
            # For the test's write interface, only write commands corresponding to an input in the following cases:
            # a) If it is not an interoperability test (which means it's an E2E test)
            # b) If it is an interoperability test and the input is valid (Use valid inputs for
            #    interoperability tests)
            if not interoperability_test or (interoperability_test
                                             and expected_values_per_ifc[wql_interface][i][1].kind == Kind.EXPRESSION):
                primary_write_interface_tables.append(i)
                wf.write('drop table if exists %s;\n' % table_name)
                wf.write("select (%s);\n" % ", ".join(table_values))
                if all(e.name != 'INTERVAL' for e in cols[wql_interface][i]):
                    columns = ["c%s %s" % (idx, col.name + col.get_argstr()) for idx, col
                               in enumerate(cols[wql_interface][i])]
                    wf.write("create table %s(%s) %s;\n" % (table_name, ", ".join(columns),
                                                            format2str(format_type)))
                    wf.write("insert into %s select %s; \n" % (table_name, ", ".join(table_values)))
                else:
                    wf.write("create table %s as select %s" % (table_name, table_values))

    for ifc in ql_interfaces:
        # This is for the Write-Write test, we only need to consider interfaces that are not the primary write interface
        # For example, for a Spark-Hive test, the primary QL interface is SparkSQL. This loop should only deal with
        # Hive's QL interface (HiveCLI).
        if ifc == wql_interface:
            continue
        tables = tables_per_ifc[ifc]
        expected_values = expected_values_per_ifc[ifc]
        with open(os.path.join(args.log_dir, 'w_'+ifc.name.lower()+"_"+format_type), 'w') as wf:
            # This extra classpath is required when Spark is used as Hive's execution engine.
            if ifc == Interface.HQL:
                hive_exec_jar_path = os.path.join(os.environ.get('HIVE_HOME'), 'lib', 'hive-exec-3.1.2.jar')
                wf.write("set spark.driver.extraClassPath=%s;\n" % hive_exec_jar_path)
                wf.write("set spark.executor.extraClassPath=%s;\n" % hive_exec_jar_path)
            # Only try to insert into tables that are created from the primary write interface
            for i in primary_write_interface_tables:
                if expected_values[i][1].kind == Kind.EXPRESSION:
                    table_name = "ws" + str(i)
                    wf.write("insert into %s select %s; \n" % (table_name, ", ".join(tables[i])))
                    wf.write("select * from %s;\n" % table_name)
    with open(os.path.join(args.log_dir, 'r_'+rql_interface.name.lower()+"_"+format_type), 'w') as wf:
        for i in range(num_tables):
            table_name = "ws"+str(i)
            wf.write("select * from %s;\n" % table_name)
    
    if wql_interface != rql_interface:
        with open(os.path.join(args.log_dir, 'r_'+wql_interface.name.lower()+"_"+format_type), 'w') as wf:
            for i in range(num_tables):
                table_name = "ws" + str(i)
                wf.write("select * from {0};\n".format(table_name))


def write_rt(tables):
    num_tables = len(tables)
    with open(os.path.join(args.log_dir, "t_expected"), 'w') as wf:
        for i in range(num_tables):
            for j in range(len(tables[i])):
                has_exn = False
                if tables[i][j].kind == Kind.EXCEPTION:
                    has_exn = True
            if not has_exn:
                tb_vals = [get_rt(x) for x in tables[i]]
                wf.write("\t".join(tb_vals)+"\n")


def write_df(cols, tables, expected, format_type, interoperability_test):
    num_tables = len(tables)
    tables_ = [row[:] for row in tables] # deep copy to avoid changing tables in-place
    with open(os.path.join(args.log_dir, "w_df_"+format_type), 'w') as wf:
        wf.write("import org.apache.spark.sql.{Row, SparkSession}\n")
        wf.write("import org.apache.spark.sql.types._\n")
        wf.write("import scala.math.BigInt\n")
        for i in range(num_tables):
            if not interoperability_test or (interoperability_test and expected[i][1].kind == Kind.EXPRESSION):
                table_name = "ws" + str(i)
                wf.write('spark.sql("drop table if exists {0};")\n'.format(table_name))
                wf.write("val rdd{0} = sc.parallelize(Seq(Row(".format(i))
                isInterval = False
                if isinstance(tables_[i][1], list): # convert list back to string and store interval
                    isInterval = True
                    interval, tables_[i][1] = tables_[i][1][1], tables_[i][1][0]
                wf.write(", ".join(tables_[i]))
                wf.write(")))\n")
                wf.write("val schema{0} = new StructType()".format(i))
                for j in range(len(cols[i])):
                    wf.write('.add(StructField("c{0}", {1}, true))'
                             .format(j, sqltype2sparktype(cols[i][j])))
                wf.write('\nval df{0} = spark.createDataFrame(rdd{1}, schema{2})\n'.format(i, i, i))
                if isInterval:
                    if 'toDF("Date")' in tables_[i][1]:
                        wf.write('val df{0}_ = df{1}.withColumn("c1", (df{2}("c1")'
                                 ' + expr("{3}")).cast(DateType))\n'.format(i, i, i, interval))
                    else:
                        wf.write('val df{0}_ = df{1}.withColumn("c1", df{2}("c1")'
                                 ' + expr("{3}"))\n'.format(i, i, i, interval))
                    wf.write('df{0}_.show(false)\n'.format(i))
                    wf.write('df{0}_.write.mode("overwrite").format("{1}").saveAsTable("{2}")\n'
                         .format(i, format_type, table_name))
                else:
                    wf.write('df{0}.show(false)\n'.format(i))
                    wf.write('df{0}.write.mode("overwrite").format("{1}").saveAsTable("{2}")\n'
                         .format(i, format_type, table_name))
    with open(os.path.join(args.log_dir, "r_df_"+format_type), 'w') as wf:
        wf.write("import org.apache.spark.sql.{Row, SparkSession}\n")
        wf.write("import org.apache.spark.sql.types._\n")
        wf.write("import scala.math.BigInt\n")
        for i in range(num_tables):
            table_name = "ws" + str(i)
            wf.write('spark.sql("select * from {0};").show(false)\n'.format(table_name))


def pack_input(interface, v1, v2):
    return {interface: v1, Interface.DF: v2}


def gen_tables(dry_run=False):
    type_interface, tables_interface, expected_interface = {}, {}, {}
    for system in SystemUnderTest:
        type_objs, entries, expected_entries = None, None, None
        for interface in system_under_test_to_interfaces[system]:
            if not all([type_objs, entries, expected_entries]):
                type_objs, entries, expected_entries = gen_entries(interface, dry_run)
            type_interface[interface] = type_objs
            tables_interface[interface] = [[x[interface] for x in y] for y in entries]
            expected_interface[interface] = [[x.values[interface] for x in y] for y in expected_entries]

    return type_interface, tables_interface, expected_interface


system_interface = {
    'spark': Interface.SQL,
    'hive': Interface.HQL
}


def sys2interface(system):
    return system_interface[system]


format_str = {
    'orc': "stored as ORC",
    'parquet': "stored as PARQUET",
    'avro': "ROW FORMAT SERDE \"org.apache.hadoop.hive.serde2.avro.AvroSerDe\" " +
            "STORED AS INPUTFORMAT \"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat\" " +
            "OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat\" "
}


def format2str(x):
    return format_str[x]


parser = argparse.ArgumentParser()
parser.add_argument('log_dir', type=str)
parser.add_argument('wsys', type=str)
parser.add_argument('rsys', type=str)
parser.add_argument('--stats', action='store_true')
parser.add_argument('--dry_run', action='store_true')
parser.add_argument('--one_way', action='store_true')
args = parser.parse_args()

# TODO: change args.system to using args.wsys or args.rsys
wql_interface = sys2interface(args.wsys)
rql_interface = sys2interface(args.rsys)
if args.stats:
    stats = get_stats(wql_interface)
    for k in stats.keys():
        print(k, "valid:", stats[k]["valid"], "invalid:", stats[k]["invalid"])
    print("total", stats["total"]["valid"] + stats["total"]["invalid"])
else:
    cols, tables, expected = gen_tables(args.dry_run)
    for _format in format_str.keys():
        write_ql(cols, tables, expected, _format, args.one_way)
        write_rt(expected[wql_interface])
        write_df(cols[Interface.DF], tables[Interface.DF], expected[Interface.DF], _format, args.one_way)
    
    write_original(cols, tables[wql_interface], expected[wql_interface], wql_interface)
