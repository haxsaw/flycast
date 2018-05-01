from datetime import datetime
from os import remove
from sqlalchemy import Integer, String, Float, Column, DateTime, ForeignKey
from sqlalchemy.orm import relationship, RelationshipProperty
from sqlalchemy.sql.schema import SchemaItem
from sqlalchemy.sql.elements import ColumnClause
from flycast import Cast, Casting, FlycastException


def rm_testdb(path):
    try:
        remove(path)
    except:
        pass


def cloner(d):
    clone = {}
    for k, v in d.items():
        if isinstance(v, (SchemaItem, ColumnClause)):
            v = v.copy()
        clone[k] = v
    return clone


def test01():
    """
    test01: Create a Cast
    """
    cast = Cast()
    assert cast, "There was no Cast instance"


user_table = {"__tablename__": "user",
              "id": Column(Integer, name="id", primary_key=True, autoincrement=True),
              "username": Column(String, name="user", nullable=False, index=True),
              "balance": Column(Float, name="balance", nullable=False, default=0.0),
              "join_date_time": Column(DateTime, name="join_date_time", nullable=False, default=datetime.now)}


def test02():
    """
    test02: Add an mapped class
    """
    c = Cast()
    r = c.add_mapped_class("User", cloner(user_table))
    assert r is c, "add_mapped_class retuned {}".format(r)


def test03():
    """
    test03: Directly create a casting
    """
    c = Casting("test03", "sqlite://", {"User": cloner(user_table)}, None, with_create=True, with_echo=True)
    assert isinstance(c, Casting), "got a {} instead of a Casting".format(c)


def test04():
    """
    test04: Create a casting and get the mapped class
    """
    c = Casting("test04", "sqlite://", {"User": cloner(user_table)}, None, with_create=True, with_echo=True,
                drop_first=True)
    mc = c.get_mapped_class("User")
    assert mc.__name__ == "User" and isinstance(mc, type), "it went wrong: {}".format(mc)


address_table = {"__tablename__": "address",
                 "id": Column(Integer, name="id", primary_key=True, nullable=False),
                 "street_address": Column(String, name="street_address", nullable=True),
                 "user_id": Column(Integer, ForeignKey("user.id"), name="user_id", nullable=False),
                 "user": relationship("User", primaryjoin="Address.user_id==User.id")}


def test05():
    """
    test05: Create two mapped tables with a relationship between the two
    """
    mt = {"User": cloner(user_table),
          "Address": cloner(address_table)}
    c = Casting("test05", "sqlite://", mt, None, with_create=True, with_echo=True)
    mc = c.get_mapped_class("Address")
    assert mc.__name__ == "Address", "wrong name: {}".format(mc)


def test06():
    """
    test06: check we raise when we get a table dict that doesn't have __tablename__ in it
    """
    t = cloner(user_table)
    del t["__tablename__"]
    try:
        c = Casting("test06", "sqlite://", {"User": t}, None, with_create=True, with_echo=True)
        assert False, "This should have raised an exception about missing __tablename__"
    except FlycastException:
        pass


def test07():
    """
    test07: check we raise when we get the same named module twice
    """
    c1 = Casting("test07", "sqlite://", {"User": cloner(user_table)}, None, with_create=True, with_echo=True)
    try:
        c2 = Casting("test07", "sqlite://", {"User": cloner(user_table)}, None, with_create=True, with_echo=True)
        assert False, "This should have raised about a duplicated module name"
    except FlycastException as e:
        assert "replace_module" in str(e), "Can't find the expected error message"


def test08():
    """
    test08: check that dup'd module names are allowed if flagged properly
    """
    c1 = Casting("test08", "sqlite://", {"User": cloner(user_table)}, None, with_create=True, with_echo=True)
    try:
        c2 = Casting("test08", "sqlite://", {"User": cloner(user_table)}, None, with_create=True, with_echo=True,
                     replace_module=True)
    except FlycastException as e:
        assert False, "this should not have raised as replace_module was specified"


def test09():
    """
    test09: create related tables and check doing a query that joins the two
    """
    mt = {"User": cloner(user_table),
          "Address": cloner(address_table)}
    c = Casting("test09", "sqlite://", mt, None, with_create=True, with_echo=True)
    User = c.get_mapped_class("User")
    Address = c.get_mapped_class("Address")
    s = c.get_session()
    r = s.query(Address).join(User).filter(User.username == "tom").all()
    assert r is not None, "no r at all??"


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test"):
            print(">>>>> starting tests {}".format(k))
            try:
                v()
            except Exception as e:
                print("---------> test {} failed with {}".format(k, str(e)))
