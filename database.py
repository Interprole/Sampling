from models import Genus, Feature, FeatureValue, LanguageFeature, DocumentLanguage, create_tables
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
    return list({language.macroarea.name for language in genus.languages if language.macroarea})


def get_all_features(source=None):
    """
    Получает список всех фичей из базы данных.
    
    Parameters
    ----------
    source : str, optional
        Фильтр по источнику ('WALS' или 'Grambank'). Если None, возвращает все.
    
    Returns
    -------
    list
        Список объектов Feature.
    """
    query = global_session.query(Feature)
    if source:
        query = query.filter_by(source=source)
    return query.all()


def get_feature_by_code(feature_code: str):
    """
    Получает фичу по её коду.
    
    Parameters
    ----------
    feature_code : str
        Код фичи (например, '1A' или 'GB020').
    
    Returns
    -------
    Feature
        Объект Feature или None, если не найдена.
    """
    return global_session.query(Feature).filter_by(code=feature_code).first()


def get_feature_values(feature_code: str):
    """
    Получает все возможные значения для заданной фичи.
    
    Parameters
    ----------
    feature_code : str
        Код фичи.
    
    Returns
    -------
    list
        Список объектов FeatureValue.
    """
    return global_session.query(FeatureValue).filter_by(feature_code=feature_code).all()


def get_language_features(glottocode: str):
    """
    Получает все фичи и их значения для заданного языка.
    
    Parameters
    ----------
    glottocode : str
        Glottocode языка.
    
    Returns
    -------
    dict
        Словарь {feature_code: value_code}.
    """
    lang_features = global_session.query(LanguageFeature).filter_by(
        language_glottocode=glottocode
    ).all()
    
    return {lf.feature_code: lf.value_code for lf in lang_features}


def get_languages_with_feature_value(feature_code: str, value_code: str):
    """
    Получает все языки, имеющие заданное значение для заданной фичи.
    
    Parameters
    ----------
    feature_code : str
        Код фичи.
    value_code : str
        Код значения.
    
    Returns
    -------
    list
        Список glottocode языков.
    """
    lang_features = global_session.query(LanguageFeature).filter_by(
        feature_code=feature_code,
        value_code=value_code
    ).all()
    
    return [lf.language_glottocode for lf in lang_features]


def get_document_languages(glottocode: str):
    """
    Получает языки источников (документации) для заданного языка.
    
    Parameters
    ----------
    glottocode : str
        Glottocode языка.
    
    Returns
    -------
    list
        Список ISO 639-3 кодов языков источников.
    """
    doc_langs = global_session.query(DocumentLanguage).filter_by(
        language_glottocode=glottocode
    ).all()
    
    return [dl.doc_language_code for dl in doc_langs]


def get_languages_with_doc_language(doc_language_code: str):
    """
    Получает все языки, имеющие документацию на заданном языке.
    
    Parameters
    ----------
    doc_language_code : str
        ISO 639-3 код языка документации (например, 'eng', 'rus').
    
    Returns
    -------
    list
        Список glottocode языков.
    """
    doc_langs = global_session.query(DocumentLanguage).filter_by(
        doc_language_code=doc_language_code
    ).all()
    
    return [dl.language_glottocode for dl in doc_langs]


def get_all_document_language_codes():
    """
    Получает список всех уникальных языков документации в базе.
    
    Returns
    -------
    list
        Список ISO 639-3 кодов.
    """
    codes = global_session.query(DocumentLanguage.doc_language_code).distinct().all()
    return [code[0] for code in codes]


def get_source_counts():
    """
    Вычисляет количество библиографических источников для каждого языка.
    
    Returns
    -------
    dict
        Словарь {glottocode: количество_источников}
    """
    from models import Source
    from sqlalchemy import func
    
    # Подсчитываем количество источников для каждого языка
    counts = global_session.query(
        Source.language_glottocode,
        func.count(Source.source).label('source_count')
    ).group_by(Source.language_glottocode).all()
    
    return {glottocode: count for glottocode, count in counts}


def get_languages_by_source(source_name):
    """
    Получает список языков имеющих определенный источник.
    
    Parameters
    ----------
    source_name : str
        Название источника (например, 'Hayward-1990a').
    
    Returns
    -------
    list
        Список glottocode языков.
    """
    from models import Source
    
    sources = global_session.query(Source).filter_by(source=source_name).all()
    return [s.language_glottocode for s in sources]