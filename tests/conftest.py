import os
import pytest
from sqlalchemy import orm
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

sqla_session_factory = orm.scoped_session(orm.sessionmaker())


@pytest.fixture(scope="function")
def db_engine():
    db_url = os.environ['DB_URL']
    db_echo = bool(os.environ['DB_ECHO'])
    assert 'test' in db_url
    engine = create_engine(db_url, echo=db_echo)
    return engine


@pytest.fixture(scope="function")
def connection(db_engine):
    connection = db_engine.connect()
    transaction = connection.begin()
    yield connection
    transaction.close()
    connection.close()


@pytest.fixture(scope="function")
def session_factory(connection):
    # Bind factories to this session maker
    sqla_session_factory.configure(bind=connection)
    db_session_factory = sqla_session_factory
    yield db_session_factory
    db_session_factory.remove()


@pytest.fixture(scope="function")
def session(session_factory):
    session = session_factory()
    session.expire_on_commit = False
    yield session
    session.close()


@pytest.fixture(scope="function")
def default_data(session):
    pass
    # create some data for tests


