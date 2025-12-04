from models import Genus, Feature, FeatureValue, LanguageFeature, create_tables
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload
from collections import Counter
import time

# Подключение к SQLite базе данных
engine = create_engine('sqlite:///sql.db')
Session = sessionmaker(bind=engine)

# Создание таблиц, если они еще не существуют
create_tables(engine)

# Глобальная сессия
global_session = Session()

# In-memory кэш (загружается из БД при первом обращении)
_macroarea_distribution_cache = None
_genus_macroareas_cache = None  # None означает, что кэш не загружен

def _load_macroarea_caches_from_db():
    """
    Загружает кэши макроареалов из базы данных.
    Если кэш в БД пуст, пересчитывает и сохраняет.
    """
    global _macroarea_distribution_cache, _genus_macroareas_cache
    
    from models import GenusMacroareaCache, MacroareaDistributionCache
    
    try:
        # Загружаем кэш распределения макроареалов
        dist_entries = global_session.query(MacroareaDistributionCache).all()
        if dist_entries:
            _macroarea_distribution_cache = {e.macroarea_name: e.genus_count for e in dist_entries}
        
        # Загружаем кэш макроареалов по родам
        genus_entries = global_session.query(GenusMacroareaCache).all()
        if genus_entries:
            _genus_macroareas_cache = {}
            for e in genus_entries:
                if e.macroareas:
                    _genus_macroareas_cache[e.genus_id] = e.macroareas.split(',')
                else:
                    _genus_macroareas_cache[e.genus_id] = []
        
        # Если кэши загружены, выходим
        if _macroarea_distribution_cache and _genus_macroareas_cache:
            return
    except Exception:
        pass
    
    # Кэш пуст или не существует - пересчитываем
    _rebuild_macroarea_caches()

def _rebuild_macroarea_caches():
    """
    Пересчитывает и сохраняет кэши макроареалов в БД.
    """
    global _macroarea_distribution_cache, _genus_macroareas_cache
    
    from models import GenusMacroareaCache, MacroareaDistributionCache, Group, Macroarea
    
    _genus_macroareas_cache = {}
    macroarea_counts = Counter()
    
    # Загружаем все роды с языками и макроареалами
    genera = global_session.query(Genus).options(
        joinedload(Genus.languages).joinedload(Group.macroarea)
    ).all()
    
    current_time = int(time.time())
    
    # Вычисляем макроареалы для каждого рода
    for genus in genera:
        macroareas = list({lang.macroarea.name for lang in genus.languages if lang.macroarea})
        _genus_macroareas_cache[genus.id] = macroareas
        
        for ma in macroareas:
            macroarea_counts[ma] += 1
    
    _macroarea_distribution_cache = dict(macroarea_counts)
    
    # Сохраняем в БД
    try:
        # Очищаем старые записи
        global_session.query(GenusMacroareaCache).delete()
        global_session.query(MacroareaDistributionCache).delete()
        
        # Сохраняем кэш макроареалов по родам
        for genus_id, macroareas in _genus_macroareas_cache.items():
            entry = GenusMacroareaCache(
                genus_id=genus_id,
                macroareas=','.join(macroareas),
                last_updated=current_time
            )
            global_session.add(entry)
        
        # Сохраняем кэш распределения
        for ma_name, count in _macroarea_distribution_cache.items():
            entry = MacroareaDistributionCache(
                macroarea_name=ma_name,
                genus_count=count,
                last_updated=current_time
            )
            global_session.add(entry)
        
        global_session.commit()
    except Exception as e:
        global_session.rollback()
        print(f"Warning: Failed to save macroarea cache to DB: {e}")

def get_genera():
    """
    Получает список всех родов (genera) из базы данных.
    
    Returns
    -------
    list
        Список объектов Genus.
    """
    return global_session.query(Genus).all()

def get_genera_with_languages():
    """
    Получает список всех родов с предзагруженными языками и макроареалами.
    Это намного быстрее, чем делать lazy loading для каждого рода отдельно.
    
    Returns
    -------
    list
        Список объектов Genus с загруженными языками.
    """
    from models import Group, Macroarea
    return global_session.query(Genus).options(
        joinedload(Genus.languages).joinedload(Group.macroarea)
    ).all()

def calculate_macroarea_distribution():
    """
    Calculates the distribution of genera across macroareas based on their associated languages.
    Использует кэш из БД для ускорения.

    Returns
    -------
    dict
        A dictionary where keys are macroarea names and values are the counts of genera in each macroarea.
    """
    global _macroarea_distribution_cache
    
    if _macroarea_distribution_cache is None:
        _load_macroarea_caches_from_db()
    
    return _macroarea_distribution_cache

def get_macroarea_by_genus(genus: Genus):
    """
    Получает название макроареала для заданного рода (genus).
    Использует кэш из БД для ускорения.

    Parameters
    ----------
    genus : Genus
        Объект рода.

    Returns
    -------
    list
        Список названий макроареалов, связанных с родом.
    """
    global _genus_macroareas_cache
    
    if _genus_macroareas_cache is None:
        _load_macroarea_caches_from_db()
    
    return _genus_macroareas_cache.get(genus.id, [])

def clear_macroarea_cache():
    """Очищает кэш макроареалов (в памяти и в БД)."""
    global _macroarea_distribution_cache, _genus_macroareas_cache
    _macroarea_distribution_cache = None
    _genus_macroareas_cache = None
    
    from models import GenusMacroareaCache, MacroareaDistributionCache
    try:
        global_session.query(GenusMacroareaCache).delete()
        global_session.query(MacroareaDistributionCache).delete()
        global_session.commit()
    except Exception:
        global_session.rollback()


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
    from models import Source
    
    sources = global_session.query(Source).filter_by(
        language_glottocode=glottocode
    ).all()
    
    # Collect all unique language codes from all sources
    codes = set()
    for source in sources:
        if source.doc_language_codes:
            codes.update(source.doc_language_codes.split(','))
    
    return list(codes)


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
    from models import Source
    
    # Find all sources that have this language code
    sources = global_session.query(Source).filter(
        Source.doc_language_codes.like(f'%{doc_language_code}%')
    ).all()
    
    # Get unique glottocodes
    return list(set(s.language_glottocode for s in sources))


def get_all_document_language_codes():
    """
    Получает список всех уникальных языков документации в базе.
    
    Returns
    -------
    list
        Список ISO 639-3 кодов.
    """
    from models import Source
    
    sources = global_session.query(Source.doc_language_codes).filter(
        Source.doc_language_codes != None
    ).all()
    
    # Collect all unique codes
    codes = set()
    for (doc_langs,) in sources:
        if doc_langs:
            codes.update(doc_langs.split(','))
    
    return sorted(list(codes))


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