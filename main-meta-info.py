from DB import DB


def get_names(db):
    db.run_test("""
        "SELECT * FROM \"Archive_val\""
    """)


def main():
    db = DB("192.168.1.2", "UPN276_prm")
    get_names(db)


main()
