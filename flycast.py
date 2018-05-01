from importlib import util, machinery
import sys

from sqlalchemy import create_engine, Column
from sqlalchemy.orm import scoped_session, sessionmaker, RelationshipProperty, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import SchemaItem
from sqlalchemy.sql.elements import ColumnClause


class FlycastException(Exception):
    pass


class Cast(object):
    def __init__(self):
        self.mapped_classes = {}
        self.modifiers = []

    def add_mapped_class(self, class_name, class_cols):
        """
        define a new mapped class
        :param class_name:
        :param class_cols:
        :return: self
        """
        self.mapped_classes[class_name] = class_cols
        return self

    def add_modifier(self, mod_func, *args, **kwargs):
        """
        captures the modifier to apply with the supplied arguments
        :param mod_func:
        :param args:
        :param kwargs:
        :return: self
        """
        self.modifiers.append((mod_func, args, kwargs))
        return self

    def make_casting(self, modname, connect_str, with_create=False, with_echo=False, drop_first=False,
                     replace_module=False):
        mapped_classes = {}
        for mapped_class_name, col_dict in self.mapped_classes.items():
            mapped_class_cols = {}
            mapped_classes[mapped_class_name] = mapped_class_cols
            for cn, col in self.mapped_classes[mapped_class_name].items():
                if isinstance(col, (SchemaItem, ColumnClause)):
                    col = col.copy()
                mapped_class_cols[cn] = col
        return Casting(modname, connect_str, mapped_classes, None, with_create=with_create,
                       with_echo=with_echo, drop_first=drop_first, replace_module=replace_module)


# def make_safe_to_reuse(val):
#     """
#     Looks type of the value, and if it certain SQLAlchemy objects, create a safe-to-use version of
#     the object. Otherwise just return the input.
#     :param val: some object that is the value of an attribute in a mapped class
#     :return: an object that is safe to use in a new mapped class
#     """
#     if isinstance(val, Column):
#         val = val.copy()
#     elif isinstance(val, RelationshipProperty):


class Casting(object):
    def __init__(self, modname, connect_str, mapped_classes, modifiers, with_create=False, with_echo=False,
                 drop_first=False, replace_module=False, **kwargs):
        if not replace_module:
            if modname in sys.modules:
                raise FlycastException("There's already a module named {} so flycast can't create "
                                       "a new one with the same name unless you make the Casting "
                                       "with replace_module=True".format(modname))

        self.session_factory = None
        self.modname = modname
        self.connect_str = connect_str
        self.mapped_classes = mapped_classes
        self.modifiers = modifiers
        self.with_build = with_create
        self.with_echo = with_echo
        self.with_delete = drop_first

        self.engine = create_engine(connect_str, echo=with_echo, **kwargs)

        # dynamically create a module
        spec = machinery.ModuleSpec(modname, None)
        self.module = util.module_from_spec(spec)
        sys.modules[self.modname] = self.module

        # set up the declarative base class
        self.module.declarative_base = declarative_base
        self.module.Base = self.module.declarative_base()

        # load the mapped classes into the module
        for mapped_class_name, class_cols in mapped_classes.items():
            if "__tablename__" not in class_cols:
                raise FlycastException("Mapped class dict {} does not have a "
                                       "__tablename__ key".format(mapped_class_name))
            setattr(self.module, mapped_class_name, type(mapped_class_name, (self.module.Base,), class_cols))

        # TODO: put the modifiers in the module here

        if with_create:
            if drop_first:
                try:
                    self.module.Base.metadata.drop_all(self.engine)
                except:
                    pass
            self.module.Base.metadata.create_all(self.engine)

        # now activate the model
        self.session_factory = scoped_session(sessionmaker(bind=self.engine))
        self.session_factory.configure(bind=self.engine)
        if self.module.Base.metadata.bind != self.engine:
            self.module.Base.metadata.bind = self.engine
        if with_echo:
            self.module.Base.metadata.bind.echo = True

    def get_session(self):
        if self.session_factory is None:
            raise FlycastException("The casting's model is not active")
        return self.session_factory()

    def get_engine(self):
        return self.engine

    def get_mapped_class(self, class_name):
        mapped_class = getattr(self.module, class_name, None)
        if mapped_class is None:
            raise FlycastException("The casting's model does not contain a mapped class named {}".format(class_name))
        return mapped_class
