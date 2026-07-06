import datetime

from DB import DB
from kks import Channels, kks_to_sql


def parce_bit(bits, index, message_if_true="", message_if_false=""):
    if not len(bits) > index:
        return ""

    if message_if_true != "":
        message_if_true = f"{message_if_true}({index}); "

    if message_if_false != "":
        message_if_false = f"{message_if_false}({index}); "
    return message_if_true if bits[index] else message_if_false


def get_paramerus_status(db):
    db.get_data(kks_to_sql("LVC60CE01_XQ01"), 12, 0,
                # date_begin=datetime.date(year=2026, month=3, day=23),
                # date_end=datetime.date(year=2026, month=3, day=22),
                time_begin=datetime.time(hour=13, minute=24, second=00, microsecond=0),
                # time_end = datetime.time(hour=18, minute=36, second=00, microsecond=0),
                callback=parce_paramerus_status,
                )


def parce_paramerus_status(val):
    if not 65535 > val > 0:
        return "ERR value"
    bits = [int(b) for b in format(int(val), '016b')]
    result = "".join(map(str, bits)) + " "
    bits = bits[::-1]

    # if val == 33026.0:
    #     return result + "ok(1,8,15)"

    result = result + parce_bit(bits, 0, "CC")
    # result = result + parce_bit(bits, 1, "remote")
    result = result + parce_bit(bits, 2, "err")
    result = result + parce_bit(bits, 3, "set=get OK")
    result = result + parce_bit(bits, 4, "over U")
    result = result + parce_bit(bits, 5, "over I")
    result = result + parce_bit(bits, 6, "over T")
    result = result + parce_bit(bits, 7, "CV")
    result = result + parce_bit(bits, 8, "", "err in conn red box and ps")
    result = result + parce_bit(bits, 9, "arc")
    result = result + parce_bit(bits, 10, "AC in")
    result = result + parce_bit(bits, 11, "short circuit")
    result = result + parce_bit(bits, 12, "HV ON")
    result = result + parce_bit(bits, 13, "EEPROM ok")
    result = result + parce_bit(bits, 14, "EEPROM err")
    # result = result + parce_bit(bits, 15, "polar")

    return result


def get_types(db):
    db.run_test("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'DBAVl_archIEC104_6_JKA20CE01_XQ01'
    """)


def main():
    db = DB("192.168.1.2", "ArchRNF")
    # db.get_all_channels()
    # get_types(db)
    # get_paramerus_status(db)

    db.get_data(Channels.ELV_U.kks_full, 12, 0,
    # db.get_data(kks_to_sql("MAJ22CP01_XQ01"), 16, 12,
    # db.get_data("DBAVl_archIEC104_7_BAA11GW01_XB01", 12, 0,
                #          date_begin=datetime.date(year=2026, month=3, day=19),
                #          date_end=datetime.date(year=2026, month=3, day=22),
                time_begin=datetime.time(hour=16, minute=5, second=00, microsecond=0),
                # time_end = datetime.time(hour=14, minute=36, second=00, microsecond=0),
                # callback=parce_paramerus_status,
                # condition=' AND "VAL" >= 0'
                )
    #####################################################
    #         db.get_values(cursor, f"""
    # SELECT "TM","TMU","VAL","ALARM" FROM "DBAVl_archIEC104_1_HVC20CE01_XQ01" WHERE "TM">'2026-03-21 14:30:40+03' AND "TM"<'2026-03-21 23:59:59+03' AND "VAL">2 AND "VAL"<3
    # """)
    ######################################################

    # except Exception as error:
    #     print(error)
    #
    # finally:
    #    del db


main()
