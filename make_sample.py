import random
import time
import heapq
from typing import List, Dict, Callable, Optional, Set
from models import Language, Genus, Macroarea, Feature, FeatureValue, LanguageFeature, Group
from database import get_genera, global_session, calculate_macroarea_distribution, get_macroarea_by_genus
from collections import Counter
from sqlalchemy.orm import joinedload
from sqlalchemy import or_

# Глобальный кэш на уровне модуля (загружается один раз)
_GLOBAL_GENUS_LIST = None  # Список родов с языками


def preload_caches():
    """
    Предзагрузка списка родов при старте приложения.
    Это ускоряет первый запрос к API.
    """
    global _GLOBAL_GENUS_LIST
    
    # Загружаем список родов с языками
    if _GLOBAL_GENUS_LIST is None:
        _GLOBAL_GENUS_LIST = global_session.query(Genus).options(
            joinedload(Genus.languages).joinedload(Group.macroarea)
        ).all()


class SamplingResult:
    """
    Класс для представления результата сэмплинга.
    """
    def __init__(self, languages: List[Language], included_genera: List[Genus], 
                 target_macroarea_distribution: Optional[Dict[str, int]] = None,
                 actual_macroarea_distribution: Optional[Dict[str, int]] = None):
        self.languages = languages
        self.included_genera = included_genera
        self.target_macroarea_distribution = target_macroarea_distribution or {}
        self.actual_macroarea_distribution = actual_macroarea_distribution or {}

    def __str__(self) -> str:
        return f"SamplingResult: {len(self.languages)} languages from {len(self.included_genera)} genera"

    def __iter__(self):
        return iter(self.languages)

    def __len__(self):
        return len(self.languages)

    def __getitem__(self, index):
        return self.languages[index]

    def summary(self):
        """
        Выводит сводку о покрытии выборки.
        """
        return {
            "num_languages": len(self.languages),
            "num_genera": len(self.included_genera),
            "genera_coverage": [genus.name for genus in self.included_genera]
        }

    def extend_sample(self, another_sample: 'SamplingResult') -> 'SamplingResult':
        """
        Extended Sample (ES): Base sample plus any additional languages included in the study.

        Parameters
        ----------
        another_sample : SamplingResult
            The additional sample to include in the extended sample.

        Returns
        -------
        SamplingResult
            The extended sample including additional languages.
        """
        new_languages = self.languages + [
            lang for lang in another_sample.languages if lang not in self.languages
        ]
        new_genera = self.included_genera + [
            genus for genus in another_sample.included_genera if genus not in self.included_genera
        ]
        return SamplingResult(new_languages, new_genera)

class Sample:
    """
    Базовый класс для сэмплов языков.
    """
    def __str__(self) -> str:
        return f"{self.__class__.__name__}"

    def summary(self):
        """
        Выводит сводку о покрытии выборки.
        """
        raise NotImplementedError("Subclasses must implement the summary method.")


class GenusSample(Sample):
    """
    Класс для сэмплов на уровне родов (genus), с поддержкой различных стратегий.
    """
    def __init__(
        self, 
        genus_list: Optional[List[Genus]] = None,
        macroareas: Optional[List[str]] = None,
        include_languages: Optional[List[str]] = None,
        exclude_languages: Optional[List[str]] = None,
        wals_features: Optional[Dict[str, List[str]]] = None,
        grambank_features: Optional[Dict[str, List[str]]] = None,
        doc_languages: Optional[List[str]] = None,
        ranking_key: Optional[str] = None,
        document_types: Optional[List[str]] = None
    ):
        """
        Инициализация GenusSample с фильтрами.
        
        Parameters
        ----------
        genus_list : List[Genus], optional
            Список родов для сэмплинга. Если None, используются все роды.
        macroareas : List[str], optional
            Список макроареалов для фильтрации (например, ['Africa', 'Eurasia']).
        include_languages : List[str], optional
            Список glottocode языков, которые обязательно должны быть включены.
        exclude_languages : List[str], optional
            Список glottocode языков, которые должны быть исключены.
        wals_features : Dict[str, List[str]], optional
            Словарь WALS фичей и их значений для фильтрации.
            Формат: {'1A': ['1.0', '2.0'], '2A': ['3.0']}
        grambank_features : Dict[str, List[str]], optional
            Словарь Grambank фичей и их значений для фильтрации.
            Формат: {'GB020': ['0', '1'], 'GB021': ['1']}
        doc_languages : List[str], optional
            Список ISO 639-3 кодов языков документации (например, ['eng', 'rus']).
            При ранжировании учитываются только источники на этих языках.
        ranking_key : str, optional
            Критерий ранжирования языков при выборе из рода.
            Возможные значения: 'source_count', 'year_ranking', 'pages_ranking', 'descriptive_ranking', None (случайный выбор).
        document_types : List[str], optional
            Список типов документов для учета при ранжировании (например, ['grammar', 'dictionary']).
            Если None или пустой, учитываются все типы документов.
        """
        if genus_list is None:
            # Используем глобальный кэш родов
            global _GLOBAL_GENUS_LIST
            if _GLOBAL_GENUS_LIST is None:
                from models import Group, Macroarea
                _GLOBAL_GENUS_LIST = global_session.query(Genus).options(
                    joinedload(Genus.languages).joinedload(Group.macroarea)
                ).all()
            self.genus_list = _GLOBAL_GENUS_LIST
        else:
            self.genus_list = genus_list
        
        self.macroareas = macroareas
        self.include_languages = set(include_languages) if include_languages else set()
        self.exclude_languages = set(exclude_languages) if exclude_languages else set()
        self.wals_features = wals_features or {}
        self.grambank_features = grambank_features or {}
        self.doc_languages = set(doc_languages) if doc_languages else set()
        self.ranking_key = ranking_key
        self.document_types = set(document_types) if document_types else set()
        
        # Генерируем ключ кэша на основе параметров ранжирования
        self._cache_key = self._generate_cache_key()
        
        # Кэш для ранжирования - динамический, зависит от document_types и doc_languages
        self._ranking_cache = {}
        
        # Кэш источников для языков (для оптимизации)
        self._sources_cache = {}
        
        # Кэш для фильтрации по фичам - загружаем все нужные данные одним запросом
        self._feature_cache = None
        if self.wals_features or self.grambank_features:
            self._load_feature_cache()
        
        # Кэш для языков документации - загружаем все данные одним запросом
        self._doc_languages_cache = None
        if self.doc_languages:
            self._load_doc_languages_cache()
        
        super().__init__()
    
    def _preload_sources_for_genus(self, genus: Genus):
        """
        Предзагружает источники для всех языков в роде одним запросом.
        Это оптимизирует ранжирование, избегая N запросов к БД.
        
        Parameters
        ----------
        genus : Genus
            Род, для языков которого нужно загрузить источники.
        """
        from models import Source
        
        if not genus.languages:
            return
        
        # Собираем glottocodes всех языков рода, которые еще не в кэше
        glottocodes = [lang.glottocode for lang in genus.languages 
                      if lang.glottocode not in self._sources_cache]
        
        if not glottocodes:
            return
        
        # Загружаем все источники для этих языков одним запросом
        query = global_session.query(Source).filter(
            Source.language_glottocode.in_(glottocodes)
        )
        
        # Применяем фильтр по языкам документации если задан
        if self.doc_languages:
            filters = []
            for doc_lang in self.doc_languages:
                filters.append(Source.doc_language_codes.like(f'%{doc_lang}%'))
            query = query.filter(or_(*filters))
        
        all_sources = query.all()
        
        # Группируем источники по языкам
        for glottocode in glottocodes:
            self._sources_cache[glottocode] = []
        
        for source in all_sources:
            self._sources_cache[source.language_glottocode].append(source)
    
    def _batch_preload_sources(self, genera: List[Genus]):
        """
        Предзагружает источники для всех языков в списке родов одним запросом.
        Это намного эффективнее чем вызывать _preload_sources_for_genus для каждого рода.
        
        Parameters
        ----------
        genera : List[Genus]
            Список родов для предзагрузки источников.
        """
        from models import Source
        
        # Собираем все glottocodes из всех родов
        all_glottocodes = []
        for genus in genera:
            if genus.languages:
                for lang in genus.languages:
                    if lang.glottocode not in self._sources_cache:
                        all_glottocodes.append(lang.glottocode)
        
        if not all_glottocodes:
            return
        
        # Один запрос для всех языков всех родов
        # ВАЖНО: выбираем все поля, чтобы избежать lazy loading
        query = global_session.query(Source).filter(
            Source.language_glottocode.in_(all_glottocodes)
        )
        
        # Применяем фильтр по языкам документации если задан
        if self.doc_languages:
            filters = []
            for doc_lang in self.doc_languages:
                filters.append(Source.doc_language_codes.like(f'%{doc_lang}%'))
            query = query.filter(or_(*filters))
        
        # Загружаем все данные сразу, чтобы не было lazy loading
        all_sources = query.all()
        
        # Принудительно загружаем все атрибуты, чтобы избежать детача от сессии
        for source in all_sources:
            # Доступ к атрибутам для загрузки
            _ = source.document_type
            _ = source.year
            _ = source.pages
        
        # Инициализируем кэш для всех glottocodes
        for glottocode in all_glottocodes:
            if glottocode not in self._sources_cache:
                self._sources_cache[glottocode] = []
        
        # Группируем источники по языкам
        for source in all_sources:
            if source.language_glottocode in self._sources_cache:
                self._sources_cache[source.language_glottocode].append(source)
    
    def _generate_cache_key(self) -> str:
        """
        Генерирует ключ кэша на основе параметров ранжирования.
        
        Returns
        -------
        str
            Хэш строка вида: ranking_method|doc_type1,doc_type2|doc_lang1,doc_lang2
        """
        import hashlib
        
        if not self.ranking_key or self.ranking_key == 'random':
            return 'random'
        
        # Сортируем для консистентности
        doc_types_str = ','.join(sorted(self.document_types)) if self.document_types else 'all'
        doc_langs_str = ','.join(sorted(self.doc_languages)) if self.doc_languages else 'all'
        
        key_string = f"{self.ranking_key}|{doc_types_str}|{doc_langs_str}"
        # Используем короткий хэш для удобства
        return hashlib.md5(key_string.encode()).hexdigest()[:16]

    def _load_feature_cache(self):
        """
        Загружает данные о фичах для всех нужных языков одним запросом.
        Оптимизирует фильтрацию по фичам.
        """
        from models import LanguageFeature
        
        # Собираем все коды фичей, которые нас интересуют
        all_feature_codes = set()
        all_feature_codes.update(self.wals_features.keys())
        all_feature_codes.update(self.grambank_features.keys())
        
        if not all_feature_codes:
            return
        
        # Загружаем все нужные данные одним запросом
        feature_data = global_session.query(LanguageFeature).filter(
            LanguageFeature.feature_code.in_(all_feature_codes)
        ).all()
        
        # Строим кэш: {glottocode: {feature_code: value_code}}
        self._feature_cache = {}
        for lf in feature_data:
            if lf.language_glottocode not in self._feature_cache:
                self._feature_cache[lf.language_glottocode] = {}
            self._feature_cache[lf.language_glottocode][lf.feature_code] = lf.value_code

    def _load_doc_languages_cache(self):
        """
        Загружает весь кэш языков документации из БД при инициализации.
        Это быстрее, чем загружать по частям во время работы.
        """
        self._doc_languages_cache = {}
        self._use_db_cache = True
        
        # Проверяем, существует ли таблица кэша
        from models import LanguageDocLanguagesCache
        try:
            # Загружаем ВСЕ записи кэша одним запросом
            # Это быстрее, чем делать много маленьких запросов потом
            cache_entries = global_session.query(LanguageDocLanguagesCache).all()
            
            if cache_entries:
                # Строим кэш из таблицы: {glottocode: set(doc_language_codes)}
                for entry in cache_entries:
                    if entry.doc_language_codes:
                        self._doc_languages_cache[entry.language_glottocode] = set(
                            entry.doc_language_codes.split(',')
                        )
                    else:
                        self._doc_languages_cache[entry.language_glottocode] = set()
                
                return  # Успешно загрузили из кэша
        except Exception:
            # Таблица не существует, используем fallback
            self._use_db_cache = False
        
        # Fallback: загружаем напрямую из источников
        # Этот код выполнится только если таблица кэша не существует
        from models import Source
        
        sources = global_session.query(Source).filter(
            Source.doc_language_codes != None,
            Source.doc_language_codes != ''
        ).all()
        
        # Строим кэш: {glottocode: set(doc_language_codes)}
        for source in sources:
            lang_code = source.language_glottocode
            if lang_code not in self._doc_languages_cache:
                self._doc_languages_cache[lang_code] = set()
            
            # Добавляем коды языков документации
            if source.doc_language_codes:
                self._doc_languages_cache[lang_code].update(source.doc_language_codes.split(','))

    def __str__(self) -> str:
        """
        Возвращает строковое представление объекта GenusSample.
        """
        return f"GenusSample with {len(self.genus_list)} genera"

    def summary(self):
        """
        Выводит сводку о родах, доступных для сэмплинга.
        """
        return {
            "num_genera": len(self.genus_list),
            "genera_names": [genus.name for genus in self.genus_list]
        }

    def get_language_rank_score(self, language: Language) -> float:
        """
        Вычисляет оценку языка для ранжирования с учетом типов документов и языков документации.
        Использует кэш из БД если доступен, иначе вычисляет и сохраняет.
        
        Parameters
        ----------
        language : Language
            Язык для оценки.
            
        Returns
        -------
        float
            Оценка языка (чем больше, тем лучше). Для случайного выбора возвращает 0.
        """
        if not self.ranking_key or self.ranking_key == 'random':
            return random.uniform(0, 1)
        
        # Проверяем кэш в памяти
        if language.glottocode in self._ranking_cache:
            return self._ranking_cache[language.glottocode]
        
        # Проверяем кэш в БД
        from models import DynamicRankingCache
        cached = global_session.query(DynamicRankingCache).filter_by(
            cache_key=self._cache_key,
            language_glottocode=language.glottocode
        ).first()
        
        if cached:
            self._ranking_cache[language.glottocode] = cached.score
            return cached.score
        
        # Вычисляем score
        from models import Source
        
        # Проверяем, есть ли источники в кэше (предзагружены для рода)
        if language.glottocode in self._sources_cache:
            sources = self._sources_cache[language.glottocode]
        else:
            # Если не предзагружено, делаем запрос (fallback)
            query = global_session.query(Source).filter(
                Source.language_glottocode == language.glottocode
            )
            
            # Фильтр по языкам документации
            if self.doc_languages:
                filters = []
                for doc_lang in self.doc_languages:
                    filters.append(Source.doc_language_codes.like(f'%{doc_lang}%'))
                query = query.filter(or_(*filters))
            
            sources = query.all()
        
        # Фильтруем по типам документов
        filtered_sources = []
        for source in sources:
            if not source.document_type:
                # Если тип не указан и не выбраны конкретные типы, включаем
                if not self.document_types:
                    filtered_sources.append(source)
                continue
            
            # Проверяем совпадение типа
            source_types = [t.strip().lower() for t in source.document_type.split(',')]
            if not self.document_types:
                # Если типы не выбраны, берем все
                filtered_sources.append(source)
            else:
                # Проверяем пересечение
                if any(st in [dt.lower() for dt in self.document_types] for st in source_types):
                    filtered_sources.append(source)
        
        # Вычисляем метрики
        source_count = len(filtered_sources)
        year_sum = sum(s.year for s in filtered_sources if s.year)
        pages_sum = sum(s.pages for s in filtered_sources if s.pages)
        
        # Вычисляем score в зависимости от метода
        if self.ranking_key == 'source_count':
            base_score = float(source_count)
        elif self.ranking_key == 'year_ranking':
            base_score = float(year_sum)
        elif self.ranking_key == 'pages_ranking':
            base_score = float(pages_sum)
        elif self.ranking_key == 'descriptive_ranking':
            # Комбинированная метрика: 0.5 * year + 2.0 * pages
            base_score = 0.5 * year_sum + 2.0 * pages_sum
        else:
            base_score = 0.0
        
        # Сохраняем в кэш памяти
        self._ranking_cache[language.glottocode] = base_score
        
        # Сохраняем в БД кэш
        try:
            cache_entry = DynamicRankingCache(
                cache_key=self._cache_key,
                language_glottocode=language.glottocode,
                score=base_score,
                last_updated=int(time.time())
            )
            global_session.merge(cache_entry)
            global_session.commit()
        except Exception:
            global_session.rollback()
        
        return base_score
    
    def _get_document_type_modifier(self, language: Language) -> float:
        """
        Вычисляет модификатор на основе типа документов и предпочтения grammar/dictionary.
        
        Parameters
        ----------
        language : Language
            Язык для оценки.
            
        Returns
        -------
        float
            Модификатор для базового score (-2.0 до +2.0).
        """
        from models import Source
        
        sources = global_session.query(Source).filter(
            Source.language_glottocode == language.glottocode,
            Source.document_type != None
        ).all()
        
        if not sources:
            return 0
        
        # Подсчитываем количество грамматик и словарей
        grammar_count = 0
        dictionary_count = 0
        
        for source in sources:
            doc_type = source.document_type.lower()
            if 'grammar' in doc_type and 'grammar_sketch' not in doc_type:
                grammar_count += 1
            elif 'dictionary' in doc_type:
                dictionary_count += 1
        
        total = grammar_count + dictionary_count
        if total == 0:
            return 0
        
        # Вычисляем соотношение: +1 если больше грамматик, -1 если больше словарей
        grammar_ratio = grammar_count / total
        dictionary_ratio = dictionary_count / total
        
        type_bias = grammar_ratio - dictionary_ratio  # От -1 до +1
        
        # Применяем preference: если у языка больше грамматик и preference положительный - усиливаем
        return self.grammar_dict_preference * type_bias

    def select_best_language(self, available_languages: List[Language]) -> Optional[Language]:
        """
        Выбирает лучший язык из списка доступных на основе критерия ранжирования.
        Добавляет небольшую случайность чтобы не всегда выбирался топ-1.
        
        Parameters
        ----------
        available_languages : List[Language]
            Список языков для выбора.
            
        Returns
        -------
        Optional[Language]
            Выбранный язык. Если ранжирование не задано, выбирается случайный язык.
        """
        if not available_languages:
            return None
        
        if len(available_languages) == 1:
            return available_languages[0]
        
        if self.ranking_key and self.ranking_key != 'random':
            # Оцениваем языки
            scored = [(self.get_language_rank_score(lang), lang) for lang in available_languages]
            scored.sort(key=lambda x: x[0], reverse=True)
            
            # Добавляем случайность: выбираем из топ-3 (если есть)
            top_n = min(3, len(scored))
            return random.choice(scored[:top_n])[1]
        else:
            # Случайный выбор
            return random.choice(available_languages)

    def select_best_genera(self, available_genera: List[Genus], count: int, macroarea_id: Optional[int] = None) -> List[Genus]:
        """
        Выбирает лучшие роды из списка доступных на основе критерия ранжирования.
        
        ОПТИМИЗАЦИЯ: Для больших списков родов мы НЕ оцениваем все роды,
        а просто берём случайную подвыборку. Ранжирование применяется только
        при выборе конкретного языка из рода.
        
        Parameters
        ----------
        available_genera : List[Genus]
            Список родов для выбора.
        count : int
            Количество родов для выбора.
        macroarea_id : int, optional
            ID макроареала (используется только для логирования).
            
        Returns
        -------
        List[Genus]
            Список выбранных родов.
        """
        if not available_genera:
            return []
        
        count = min(count, len(available_genera))
        
        # РАДИКАЛЬНАЯ ОПТИМИЗАЦИЯ: всегда используем случайный выбор родов
        # Ранжирование применяется только при выборе языка из рода
        return random.sample(available_genera, count)


    def limit_languages(self, languages: List[Language], num_languages: int) -> List[Language]:
        """
        Ограничивает количество языков до указанного числа, выбирая случайно.
        """
        if len(languages) > num_languages:
            return random.sample(languages, num_languages)
        return languages

    def get_languages_from_genus(self, genus: Genus) -> List[Language]:
        """
        Вспомогательный метод, возвращающий список языков в данном роде.
        """
        return genus.languages if genus.languages else []
    
    def filter_languages_by_features(self, languages: List[Language]) -> List[Language]:
        """
        Фильтрует языки по заданным WALS и Grambank фичам.
        
        Parameters
        ----------
        languages : List[Language]
            Список языков для фильтрации.
            
        Returns
        -------
        List[Language]
            Отфильтрованный список языков.
        """
        if not self.wals_features and not self.grambank_features:
            return languages
        
        # Если кэш не загружен (не должно быть), загружаем
        if self._feature_cache is None:
            self._load_feature_cache()
        
        filtered = []
        
        for lang in languages:
            # Получаем фичи языка из кэша
            lang_feature_dict = self._feature_cache.get(lang.glottocode, {})
            
            # Проверяем WALS фичи
            wals_match = True
            for feature_code, allowed_values in self.wals_features.items():
                lang_value = lang_feature_dict.get(feature_code)
                if lang_value is None or lang_value not in allowed_values:
                    wals_match = False
                    break
            
            # Проверяем Grambank фичи
            grambank_match = True
            for feature_code, allowed_values in self.grambank_features.items():
                lang_value = lang_feature_dict.get(feature_code)
                if lang_value is None or lang_value not in allowed_values:
                    grambank_match = False
                    break
            
            if wals_match and grambank_match:
                filtered.append(lang)
        
        return filtered
    
    def filter_languages_by_macroarea(self, languages: List[Language]) -> List[Language]:
        """
        Фильтрует языки по макроареалам.
        
        Parameters
        ----------
        languages : List[Language]
            Список языков для фильтрации.
            
        Returns
        -------
        List[Language]
            Отфильтрованный список языков.
        """
        if not self.macroareas:
            return languages
        
        filtered = []
        for lang in languages:
            if lang.macroarea and lang.macroarea.name in self.macroareas:
                filtered.append(lang)
        
        return filtered
    
    def filter_languages_by_doc_languages(self, languages: List[Language]) -> List[Language]:
        """
        Фильтрует языки по языкам документации.
        
        Parameters
        ----------
        languages : List[Language]
            Список языков для фильтрации.
            
        Returns
        -------
        List[Language]
            Отфильтрованный список языков.
        """
        if not self.doc_languages:
            return languages
        
        # Преобразуем список требуемых языков документации в множество
        required_doc_langs = set(self.doc_languages)
        
        # Фильтруем языки используя кэш (уже полностью загружен при инициализации)
        filtered = []
        for lang in languages:
            lang_doc_codes = self._doc_languages_cache.get(lang.glottocode, set())
            
            # Проверяем, есть ли пересечение с требуемыми языками документации
            if lang_doc_codes & required_doc_langs:  # Пересечение множеств
                filtered.append(lang)
        
        return filtered
    
    def apply_include_exclude(self, languages: List[Language]) -> List[Language]:
        """
        Применяет фильтры исключения языков (include_languages обрабатываются отдельно).
        
        Parameters
        ----------
        languages : List[Language]
            Список языков для фильтрации.
            
        Returns
        -------
        List[Language]
            Отфильтрованный список языков.
        """
        # Исключаем языки из exclude_languages
        filtered = [lang for lang in languages if lang.glottocode not in self.exclude_languages]
        
        return filtered
    
    def get_included_languages(self) -> List[Language]:
        """
        Получает языки, которые должны быть обязательно включены в выборку.
        
        Returns
        -------
        List[Language]
            Список языков для обязательного включения.
        """
        if not self.include_languages:
            return []
        
        included = []
        for glottocode in self.include_languages:
            lang = global_session.query(Language).filter_by(glottocode=glottocode).first()
            if lang:
                included.append(lang)
        
        return included
    
    def genus_has_available_languages(self, genus: Genus) -> bool:
        """
        Проверяет, есть ли у рода языки, удовлетворяющие всем фильтрам.
        
        Parameters
        ----------
        genus : Genus
            Род для проверки.
            
        Returns
        -------
        bool
            True если есть хотя бы один подходящий язык.
        """
        if not genus.languages:
            return False
        
        available_languages = self.apply_all_filters(genus.languages)
        return len(available_languages) > 0
    
    def apply_all_filters(self, languages: List[Language]) -> List[Language]:
        """
        Применяет все фильтры к списку языков.
        
        Parameters
        ----------
        languages : List[Language]
            Список языков для фильтрации.
            
        Returns
        -------
        List[Language]
            Отфильтрованный список языков.
        """
        languages = self.filter_languages_by_macroarea(languages)
        languages = self.filter_languages_by_doc_languages(languages)
        languages = self.filter_languages_by_features(languages)
        languages = self.apply_include_exclude(languages)
        
        return languages

    def genus_sample(self) -> SamplingResult:
        """
        Genus Sample (GS): One language from every genus.
        Применяет все фильтры и добавляет обязательные языки.
        """
        selected_languages = []
        included_genera = []
        
        # Сначала добавляем обязательные языки
        mandatory_languages = self.get_included_languages()
        mandatory_glottocodes = {lang.glottocode for lang in mandatory_languages}
        selected_languages.extend(mandatory_languages)
        
        # Добавляем роды обязательных языков
        for lang in mandatory_languages:
            if lang.genus and lang.genus not in included_genera:
                included_genera.append(lang.genus)
        
        for genus in self.genus_list:
            # Пропускаем роды, которые уже представлены обязательными языками
            if genus in included_genera:
                continue
            
            if genus.languages:
                # Применяем все фильтры к языкам рода
                available_languages = self.apply_all_filters(genus.languages)
                
                if available_languages:
                    # Исключаем уже добавленные обязательные языки
                    available_languages = [
                        lang for lang in available_languages 
                        if lang.glottocode not in mandatory_glottocodes
                    ]
                    
                    if available_languages:
                        selected_languages.append(self.select_best_language(available_languages))
                        included_genera.append(genus)
        
        return SamplingResult(selected_languages, included_genera)

    def core_sample(self) -> SamplingResult:
        """
        Core Sample (CS): One language from every genus with usable sources of data.
        Применяет все фильтры и добавляет обязательные языки.
        """
        selected_languages = []
        included_genera = []
        
        # Сначала добавляем обязательные языки
        mandatory_languages = self.get_included_languages()
        mandatory_glottocodes = {lang.glottocode for lang in mandatory_languages}
        selected_languages.extend(mandatory_languages)
        
        # Добавляем роды обязательных языков
        for lang in mandatory_languages:
            if lang.genus and lang.genus not in included_genera:
                included_genera.append(lang.genus)
        
        for genus in self.genus_list:
            # Пропускаем роды, которые уже представлены обязательными языками
            if genus in included_genera:
                continue
            
            #usable_languages = [lang for lang in genus.languages if lang.usable_sources]
            usable_languages = genus.languages if genus.languages else []
            
            # Применяем все фильтры
            usable_languages = self.apply_all_filters(usable_languages)

            if usable_languages:
                # Исключаем уже добавленные обязательные языки
                usable_languages = [
                    lang for lang in usable_languages 
                    if lang.glottocode not in mandatory_glottocodes
                ]
                
                if usable_languages:
                    selected_languages.append(self.select_best_language(usable_languages))
                    included_genera.append(genus)
        
        return SamplingResult(selected_languages, included_genera)

    def restricted_sample(self) -> SamplingResult:
        """
        Restricted Sample (RS): Subsample of CS with the same genealogical diversity distribution as macroarea_distribution.
        Применяет все фильтры и добавляет обязательные языки.
        """
        # Если заданы конкретные макроареалы, используем их для распределения
        if self.macroareas:
            macroarea_distribution = {area: 0 for area in self.macroareas}
            for genus in self.genus_list:
                macroareas = get_macroarea_by_genus(genus)
                for macroarea in macroareas:
                    if macroarea in self.macroareas:
                        macroarea_distribution[macroarea] += 1
        else:
            macroarea_distribution = calculate_macroarea_distribution()
        
        total_genera = sum(macroarea_distribution.values())
        if total_genera == 0:
            return SamplingResult([], [])
        
        cs_result = self.core_sample()

        # Calculate the target number of genera for each macroarea based on proportions
        target_distribution = {
            macroarea: round(len(cs_result.included_genera) * (count / total_genera))
            for macroarea, count in macroarea_distribution.items()
        }

        macroarea_genera = {area: [] for area in macroarea_distribution.keys()}
        for genus in cs_result.included_genera:
            # Проверяем, есть ли у рода языки после фильтрации
            if not self.genus_has_available_languages(genus):
                continue
                
            macroareas = get_macroarea_by_genus(genus)
            for macroarea in macroareas:
                if macroarea in macroarea_genera:
                    macroarea_genera[macroarea].append(genus)

        selected_languages = []
        included_genera = []
        
        # Добавляем обязательные языки
        mandatory_languages = self.get_included_languages()
        mandatory_glottocodes = {lang.glottocode for lang in mandatory_languages}
        selected_languages.extend(mandatory_languages)
        
        for lang in mandatory_languages:
            if lang.genus and lang.genus not in included_genera:
                included_genera.append(lang.genus)
        
        # Первый проход: распределяем по макроареалам
        for macroarea, target_count in target_distribution.items():
            available_genera = [g for g in macroarea_genera.get(macroarea, []) if g not in included_genera]
            selected_genera = available_genera[:target_count]
            
            for genus in selected_genera:
                if genus.languages:
                    available_languages = self.apply_all_filters(genus.languages)
                    available_languages = [
                        lang for lang in available_languages 
                        if lang.glottocode not in mandatory_glottocodes
                    ]
                    
                    if available_languages:
                        selected_languages.append(self.select_best_language(available_languages))
                        included_genera.append(genus)

        return SamplingResult(selected_languages, included_genera)

    def primary_sample(self, sample_size: int) -> SamplingResult:
        """
        Primary Sample (PS): Predetermined sample size with equal genealogical diversity in each macroarea.
        Применяет все фильтры и добавляет обязательные языки.
        
        Parameters
        ----------
        sample_size : int
            Желаемый размер выборки.
        """
        # Инициализируем генератор случайных чисел для каждого нового сэмпла
        random.seed()
        
        # Перемешиваем список родов для случайности
        shuffled_genus_list = self.genus_list.copy()
        random.shuffle(shuffled_genus_list)
        
        # Если заданы конкретные макроареалы, используем их для распределения
        if self.macroareas:
            macroarea_distribution = {area: 0 for area in self.macroareas}
            for genus in shuffled_genus_list:
                macroareas = get_macroarea_by_genus(genus)
                for macroarea in macroareas:
                    if macroarea in self.macroareas:
                        macroarea_distribution[macroarea] += 1
        else:
            macroarea_distribution = calculate_macroarea_distribution()
        
        total_genera = sum(macroarea_distribution.values())
        if total_genera == 0:
            return SamplingResult([], [])
        
        # Получаем обязательные языки
        mandatory_languages = self.get_included_languages()
        mandatory_glottocodes = {lang.glottocode for lang in mandatory_languages}
        mandatory_genera = []
        
        for lang in mandatory_languages:
            if lang.genus and lang.genus not in mandatory_genera:
                mandatory_genera.append(lang.genus)
        
        # Обязательные языки входят в общий размер выборки
        # Оставшееся место = запрошенный размер - количество обязательных языков
        remaining_size = max(0, sample_size - len(mandatory_languages))

        # Calculate the target number of genera for each macroarea based on proportions
        # Используем полный sample_size для целевого распределения, чтобы сравнение с actual было корректным
        target_distribution = {
            macroarea: round(sample_size * (count / total_genera))
            for macroarea, count in macroarea_distribution.items()
        }
        
        # Корректируем распределение, чтобы сумма была точно equal sample_size
        current_total = sum(target_distribution.values())
        if current_total != sample_size and target_distribution:
            # Находим макроареал с наибольшим количеством родов для корректировки
            largest_macroarea = max(macroarea_distribution.keys(), key=lambda m: macroarea_distribution[m])
            target_distribution[largest_macroarea] += (sample_size - current_total)

        macroarea_genera = {area: [] for area in macroarea_distribution.keys()}
        for genus in shuffled_genus_list:
            if genus in mandatory_genera:
                continue
            
            # Проверяем, есть ли у рода языки после фильтрации
            if not self.genus_has_available_languages(genus):
                continue
            
            macroareas = get_macroarea_by_genus(genus)
            for macroarea in macroareas:
                if macroarea in macroarea_genera:
                    macroarea_genera[macroarea].append(genus)

        selected_languages = list(mandatory_languages)
        included_genera = list(mandatory_genera)
        
        # Предзагружаем ID макроареалов для оптимизации
        macroarea_id_map = {}
        for macroarea_name in macroarea_distribution.keys():
            macroarea_obj = global_session.query(Macroarea).filter_by(name=macroarea_name).first()
            if macroarea_obj:
                macroarea_id_map[macroarea_name] = macroarea_obj.id
        
        # Первый проход: распределяем по макроареалам согласно целевому распределению
        for macroarea, target_count in target_distribution.items():
            # Исключаем уже использованные роды
            available_genera = [g for g in macroarea_genera.get(macroarea, []) if g not in included_genera]
            num_to_select = min(target_count, len(available_genera))
            
            if num_to_select > 0:
                # Используем предзагруженный ID макроареала
                macroarea_id = macroarea_id_map.get(macroarea)
                selected_genera = self.select_best_genera(available_genera, num_to_select, macroarea_id)
                
                for genus in selected_genera:
                    if genus.languages:
                        available_languages = self.apply_all_filters(genus.languages)
                        # Фильтруем языки по текущему макроареалу
                        available_languages = [
                            lang for lang in available_languages 
                            if lang.glottocode not in mandatory_glottocodes
                            and lang.macroarea and lang.macroarea.name == macroarea
                        ]
                        
                        if available_languages:
                            selected_languages.append(self.select_best_language(available_languages))
                            included_genera.append(genus)
        
        # Циклический проход: добираем недостающие языки с сохранением пропорций макроареалов
        max_iterations = 1000  # Защита от бесконечного цикла
        iteration = 0
        
        while iteration < max_iterations:
            shortage = remaining_size - (len(selected_languages) - len(mandatory_languages))
            
            # Если достигли нужного размера или нет дефицита, выходим
            if shortage <= 0:
                break
            
            # Проходим по макроареалам в том же порядке, пытаясь добрать языки
            languages_added = False
            
            for macroarea in target_distribution.keys():
                if shortage <= 0:
                    break
                
                # Находим оставшиеся роды в этом макроареале
                available_genera = [
                    g for g in macroarea_genera.get(macroarea, []) 
                    if g not in included_genera
                ]
                
                if not available_genera:
                    continue
                
                # Используем предзагруженный ID макроареала
                macroarea_id = macroarea_id_map.get(macroarea)
                
                # Выбираем один лучший род из оставшихся
                selected_genera = self.select_best_genera(available_genera, 1, macroarea_id)
                
                for genus in selected_genera:
                    if genus.languages:
                        available_languages = self.apply_all_filters(genus.languages)
                        # Фильтруем языки по текущему макроареалу
                        available_languages = [
                            lang for lang in available_languages 
                            if lang.glottocode not in mandatory_glottocodes
                            and lang.macroarea and lang.macroarea.name == macroarea
                        ]
                        
                        if available_languages:
                            selected_languages.append(self.select_best_language(available_languages))
                            included_genera.append(genus)
                            languages_added = True
                            shortage -= 1
                            
                            if shortage <= 0:
                                break
            
            # Если не добавили ни одного языка во всех макроареалах, выходим
            if not languages_added:
                break
            
            iteration += 1
        
        # Дополнительный проход: если родов меньше чем sample_size, добираем из уже использованных родов
        selected_glottocodes = {lang.glottocode for lang in selected_languages}
        shortage = sample_size - len(selected_languages)
        
        if shortage > 0:
            # Собираем все доступные языки из всех родов (включая уже использованные)
            all_available_languages = []
            for genus in self.genus_list:
                if genus.languages:
                    available = self.apply_all_filters(genus.languages)
                    for lang in available:
                        if lang.glottocode not in selected_glottocodes and lang.glottocode not in mandatory_glottocodes:
                            all_available_languages.append(lang)
            
            # Сортируем по рангу и добираем
            if all_available_languages and self.ranking_key:
                scored = [(lang, self.get_language_rank_score(lang)) for lang in all_available_languages]
                scored.sort(key=lambda x: x[1], reverse=True)
                additional = [lang for lang, score in scored[:shortage]]
            elif all_available_languages:
                additional = random.sample(all_available_languages, min(shortage, len(all_available_languages)))
            else:
                additional = []
            
            for lang in additional:
                selected_languages.append(lang)
                if lang.genus and lang.genus not in included_genera:
                    included_genera.append(lang.genus)
        
        # Подсчитываем фактическое распределение по макроареалам
        # Каждый язык считается по своему собственному макроареалу
        actual_distribution = {}
        for lang in selected_languages:
            if lang.macroarea:
                ma_name = lang.macroarea.name
                actual_distribution[ma_name] = actual_distribution.get(ma_name, 0) + 1

        return SamplingResult(
            selected_languages, 
            included_genera,
            target_macroarea_distribution=target_distribution,
            actual_macroarea_distribution=actual_distribution
        )

    def random_sample(self, sample_size: int) -> SamplingResult:
        """
        Random Sample (RS): Predetermined sample size with random selection of genera.
        В отличие от primary_sample, не учитывает пропорциональное распределение по макроареалам.
        Применяет все фильтры и добавляет обязательные языки.
        
        Parameters
        ----------
        sample_size : int
            Желаемый размер выборки.
        """
        # Инициализируем генератор случайных чисел для каждого нового сэмпла
        random.seed()
        
        # Перемешиваем список родов для случайности
        shuffled_genus_list = self.genus_list.copy()
        random.shuffle(shuffled_genus_list)
        
        # Получаем обязательные языки
        mandatory_languages = self.get_included_languages()
        mandatory_glottocodes = {lang.glottocode for lang in mandatory_languages}
        mandatory_genera = []
        
        for lang in mandatory_languages:
            if lang.genus and lang.genus not in mandatory_genera:
                mandatory_genera.append(lang.genus)
        
        # Обязательные языки входят в общий размер выборки
        remaining_size = max(0, sample_size - len(mandatory_languages))
        
        # Собираем все доступные роды (после применения фильтров)
        available_genera = []
        for genus in shuffled_genus_list:
            if genus in mandatory_genera:
                continue
            
            # Проверяем, есть ли у рода языки после фильтрации
            if self.genus_has_available_languages(genus):
                available_genera.append(genus)
        
        selected_languages = list(mandatory_languages)
        included_genera = list(mandatory_genera)
        
        # Циклически выбираем роды и языки пока не достигнем нужного размера
        max_iterations = 1000  # Защита от бесконечного цикла
        iteration = 0
        
        while iteration < max_iterations:
            # Проверяем текущий размер выборки (без mandatory)
            current_size = len(selected_languages) - len(mandatory_languages)
            shortage = remaining_size - current_size
            
            # Если достигли нужного размера или нет дефицита, выходим
            if shortage <= 0:
                break
            
            # Фильтруем доступные роды (исключаем уже использованные)
            remaining_genera = [g for g in available_genera if g not in included_genera]
            
            # Если больше нет доступных родов, выходим
            if not remaining_genera:
                break
            
            # Выбираем нужное количество родов
            num_to_select = min(shortage, len(remaining_genera))
            selected_genera = self.select_best_genera(remaining_genera, num_to_select)
            
            # Отслеживаем, добавили ли мы хотя бы один язык
            languages_added = False
            
            for genus in selected_genera:
                if genus.languages:
                    available_languages = self.apply_all_filters(genus.languages)
                    available_languages = [
                        lang for lang in available_languages 
                        if lang.glottocode not in mandatory_glottocodes
                    ]
                    
                    if available_languages:
                        selected_languages.append(self.select_best_language(available_languages))
                        included_genera.append(genus)
                        languages_added = True
            
            # Если не добавили ни одного языка, выходим
            if not languages_added:
                break
            
            iteration += 1
        
        return SamplingResult(selected_languages, included_genera)
    
    def diversity_value_sample(self, sample_size: int) -> SamplingResult:
        """
        Diversity Value (DV) Sample: Uses genealogical tree structure to ensure maximum diversity.
        
        The DV method calculates diversity values for each node in the genealogical tree based on
        the complexity of the subtree beneath it. Languages are then selected proportionally to
        these diversity values to maximize typological variety.
        
        Based on Rijkhoff et al. (1993) and Rijkhoff & Bakker (1998).
        
        Parameters
        ----------
        sample_size : int
            Desired sample size.
            
        Returns
        -------
        SamplingResult
            The resulting sample with selected languages and their genera.
        """
        from collections import defaultdict
        import math
        
        # Инициализируем генератор случайных чисел
        random.seed()
        
        # Получаем обязательные языки
        mandatory_languages = self.get_included_languages()
        mandatory_glottocodes = {lang.glottocode for lang in mandatory_languages}
        
        # Загружаем все группы из базы данных (включая промежуточные узлы)
        all_groups = global_session.query(Group).all()
        
        # Применяем фильтры к языкам
        all_languages = [g for g in all_groups if g.is_language]
        available_languages = self.apply_all_filters(all_languages)
        available_glottocodes = {lang.glottocode for lang in available_languages}
        
        if len(available_glottocodes) == 0:
            return SamplingResult(list(mandatory_languages), [])
        
        # Строим дерево: для каждого узла определяем детей и языки
        tree_structure = defaultdict(lambda: {'children': set(), 'parent': None, 'is_language': False, 'languages': []})
        
        for group in all_groups:
            glottocode = group.glottocode
            
            # Инициализируем узел
            tree_structure[glottocode]['is_language'] = group.is_language
            
            # Если это доступный язык, добавляем его
            if group.is_language and glottocode in available_glottocodes:
                tree_structure[glottocode]['languages'].append(glottocode)
            
            # Устанавливаем связь родитель-ребенок
            if group.closest_supergroup:
                parent = group.closest_supergroup
                tree_structure[parent]['children'].add(glottocode)
                tree_structure[glottocode]['parent'] = parent
                
                # Языки "поднимаются" к родительским узлам для удобства выборки
                if group.is_language and glottocode in available_glottocodes:
                    # Добавляем язык ко всем предкам
                    current = parent
                    while current:
                        tree_structure[current]['languages'].append(glottocode)
                        current = tree_structure[current]['parent']
        
        # Вычисляем глубину каждого узла (расстояние от корня)
        def calculate_depth(node):
            """Рекурсивно вычисляет глубину узла."""
            if 'depth' in tree_structure[node]:
                return tree_structure[node]['depth']
            
            parent = tree_structure[node]['parent']
            if parent is None:
                depth = 0
            else:
                depth = calculate_depth(parent) + 1
            
            tree_structure[node]['depth'] = depth
            return depth
        
        for node in tree_structure.keys():
            calculate_depth(node)
        
        # Вычисляем Diversity Value для каждого узла рекурсивно
        def calculate_dv(node, memo=None):
            """
            Recursively calculate diversity value for a node.
            DV is based on the complexity of the subtree: number of branches and their depths.
            Higher levels (closer to root) get greater weights.
            """
            if memo is None:
                memo = {}
            
            if node in memo:
                return memo[node]
            
            children = tree_structure[node]['children']
            
            if not children:
                # Leaf node has DV = 1
                dv = 1.0
            else:
                # For internal nodes: DV = sum of children's DVs weighted by depth
                child_dvs = [calculate_dv(child, memo) for child in children]
                num_children = len(children)
                depth = tree_structure[node].get('depth', 0)
                
                # Weight formula: nodes closer to root (lower depth) get higher weight
                # Using exponential decay: deeper nodes have less weight
                weight = math.pow(2, max(0, 10 - depth))  # Max depth assumed ~10
                
                # DV = number of children * weight * average child DV
                if child_dvs:
                    avg_child_dv = sum(child_dvs) / len(child_dvs)
                    dv = num_children * weight * avg_child_dv
                else:
                    dv = weight
            
            memo[node] = dv
            return dv
        
        # Находим корневые узлы (top-level families) - те, у кого нет родителя
        root_nodes = [node for node, data in tree_structure.items() 
                     if data['parent'] is None and data['languages']]
        
        if not root_nodes:
            # Если не нашли корни, берем узлы с максимальной глубиной
            return SamplingResult(list(mandatory_languages), [])
        
        # Вычисляем DV для всех корневых узлов
        family_dvs = {}
        for root in root_nodes:
            dv = calculate_dv(root)
            if dv > 0 and tree_structure[root]['languages']:
                family_dvs[root] = dv
        
        total_dv = sum(family_dvs.values())
        
        if total_dv == 0:
            return SamplingResult(list(mandatory_languages), [])
        
        # Определяем количество языков из каждой семьи пропорционально DV
        remaining_size = max(0, sample_size - len(mandatory_languages))
        
        family_allocations = {}
        allocated_total = 0
        
        for family, dv in sorted(family_dvs.items(), key=lambda x: x[1], reverse=True):
            allocation = round((dv / total_dv) * remaining_size)
            family_allocations[family] = allocation
            allocated_total += allocation
        
        # Корректируем, чтобы сумма была точно равна remaining_size
        diff = remaining_size - allocated_total
        if diff != 0 and family_allocations:
            largest_family = max(family_dvs.keys(), key=lambda f: family_dvs[f])
            family_allocations[largest_family] = max(0, family_allocations[largest_family] + diff)
        
        # Рекурсивно распределяем языки по дереву
        def allocate_languages_in_subtree(node, num_to_allocate):
            """
            Recursively allocate languages to nodes in subtree proportionally to their DVs.
            Returns list of selected language glottocodes.
            """
            if num_to_allocate <= 0:
                return []
            
            children = tree_structure[node]['children']
            node_languages = tree_structure[node]['languages']
            
            # Если нет языков в этом поддереве
            if not node_languages:
                return []
            
            # Если это листовой узел или язык
            if not children or tree_structure[node]['is_language']:
                # Выбираем из доступных языков узла (с учетом ранжирования, если задано)
                available = [g for g in node_languages if g not in mandatory_glottocodes]
                if available:
                    num_select = min(num_to_allocate, len(available))
                    
                    # Преобразуем glottocodes в объекты Language для ранжирования
                    available_lang_objects = []
                    for glottocode in available:
                        lang = global_session.query(Language).filter_by(glottocode=glottocode).first()
                        if lang:
                            available_lang_objects.append(lang)
                    
                    if not available_lang_objects:
                        return []
                    
                    # Применяем ранжирование если задано, иначе случайный выбор
                    if self.ranking_key:
                        # Сортируем по рангу и выбираем топ N
                        scored = [(lang, self.get_language_rank_score(lang)) for lang in available_lang_objects]
                        scored.sort(key=lambda x: x[1], reverse=True)
                        selected = [lang.glottocode for lang, score in scored[:num_select]]
                    else:
                        # Случайный выбор
                        selected = [lang.glottocode for lang in random.sample(available_lang_objects, num_select)]
                    
                    return selected
                return []
            
            # Если есть дети, распределяем между ними пропорционально их DV
            if children:
                # Фильтруем детей, у которых есть доступные языки
                children_with_langs = [c for c in children if tree_structure[c]['languages']]
                
                if not children_with_langs:
                    return []
                
                child_dvs = {child: calculate_dv(child) for child in children_with_langs}
                total_child_dv = sum(child_dvs.values())
                
                if total_child_dv == 0:
                    return []
                
                selected = []
                allocated = 0
                
                # Перемешиваем детей для случайности при равных DV
                shuffled_children = list(children_with_langs)
                random.shuffle(shuffled_children)
                
                # Распределяем пропорционально DV детей (с учетом перемешивания)
                sorted_children = sorted(shuffled_children, key=lambda c: child_dvs[c], reverse=True)
                
                for i, child in enumerate(sorted_children):
                    if i == len(sorted_children) - 1:
                        # Последнему ребенку отдаем остаток
                        child_allocation = num_to_allocate - allocated
                    else:
                        child_allocation = round((child_dvs[child] / total_child_dv) * num_to_allocate)
                    
                    if child_allocation > 0:
                        selected.extend(allocate_languages_in_subtree(child, child_allocation))
                        allocated += child_allocation
                
                return selected
            
            return []
        
        # Собираем выбранные языки из каждой семьи
        selected_glottocodes = set()
        
        for family, allocation in family_allocations.items():
            if allocation > 0:
                family_langs = allocate_languages_in_subtree(family, allocation)
                selected_glottocodes.update(family_langs)
        
        # Циклически добираем недостающие языки пока есть доступные
        max_iterations = 1000  # Защита от бесконечного цикла
        iteration = 0
        
        while iteration < max_iterations:
            shortage = remaining_size - len(selected_glottocodes)
            
            # Если достигли нужного размера, выходим
            if shortage <= 0:
                break
            
            remaining = available_glottocodes - selected_glottocodes - mandatory_glottocodes
            
            # Если больше нет доступных языков, выходим
            if not remaining:
                break
            
            # Преобразуем glottocodes в объекты Language
            remaining_lang_objects = []
            for glottocode in remaining:
                lang = global_session.query(Language).filter_by(glottocode=glottocode).first()
                if lang:
                    remaining_lang_objects.append(lang)
            
            if not remaining_lang_objects:
                break
            
            num_additional = min(shortage, len(remaining_lang_objects))
            
            # Применяем ранжирование если задано, иначе случайный выбор
            if self.ranking_key:
                # Сортируем по рангу и выбираем топ N
                scored = [(lang, self.get_language_rank_score(lang)) for lang in remaining_lang_objects]
                scored.sort(key=lambda x: x[1], reverse=True)
                additional = [lang.glottocode for lang, score in scored[:num_additional]]
            else:
                # Случайный выбор
                additional = [lang.glottocode for lang in random.sample(remaining_lang_objects, num_additional)]
            
            # Если не добавили ни одного языка, выходим
            if not additional:
                break
            
            selected_glottocodes.update(additional)
            iteration += 1
        
        # Преобразуем glottocodes в объекты Language
        selected_languages = list(mandatory_languages)
        included_genera = []
        
        for glottocode in selected_glottocodes:
            lang = global_session.query(Language).filter_by(glottocode=glottocode).first()
            if lang:
                selected_languages.append(lang)
                if lang.genus and lang.genus not in included_genera:
                    included_genera.append(lang.genus)
        
        return SamplingResult(selected_languages, included_genera)
    
    