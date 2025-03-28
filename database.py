from models import Genus, create_tables
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from collections import Counter

# Подключение к SQLite базе данных
engine = create_engine('sqlite:///sql.db')
Session = sessionmaker(bind=engine)

# Создание таблиц, если они еще не существуют
create_tables(engine)

# Глобальная сессия
global_session = Session()

def get_genera():
    """
    Получает список всех родов (genera) из базы данных.
    
    Returns
    -------
    list
        Список объектов Genus.
    """
    return global_session.query(Genus).all()

def calculate_macroarea_distribution():
    """
    Calculates the distribution of genera across macroareas based on their associated languages.

    Returns
    -------
    dict
        A dictionary where keys are macroarea names and values are the counts of genera in each macroarea.
    """
    macroarea_counts = Counter()
    genera = get_genera()

    for genus in genera:
        macroareas = get_macroarea_by_genus(genus)
        for macroarea in macroareas:
            macroarea_counts[macroarea] += 1

    return dict(macroarea_counts)

def get_macroarea_by_genus(genus: Genus):
    """
    Получает название макроареала для заданного рода (genus).

    Parameters
    ----------
    genus : str
        Название рода (genus).

    Returns
    -------
    list
        Список названий макроареалов, связанных с родом.
    """
    return list({language.macroarea for language in genus.languages if language.macroarea})