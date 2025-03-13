import pytest

from src.diffa.db.factory import DatabaseFactory
from src.diffa.db.diffa import DiffaCheckDatabase
from src.diffa.config import ConfigManager

@pytest.fixture(scope="module")
def config():
    return ConfigManager()

@pytest.fixture(scope="module")
def source_database(config):
    db = DatabaseFactory.create_database(config.get_db_config('source'))
    db.connect()
    yield db
    db.close()

@pytest.fixture(scope="module")
def target_database(config):
    db = DatabaseFactory.create_database(config.get_db_config('target'))
    db.connect()
    yield db
    db.close()

@pytest.fixture(scope="module")
def diffa_database(config):
    db = DiffaCheckDatabase(config.get_db_config('diffa', table_key="diffa"))
    db.connect()
    yield db
    db.close()

def test_source_database(source_database):
    assert source_database is not None

def test_target_database(target_database):
    assert target_database is not None

def test_diffa_database(diffa_database):
    assert diffa_database is not None