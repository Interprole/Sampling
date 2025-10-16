import random
from typing import List, Dict, Callable, Optional, Set
from models import Language, Genus, Macroarea, Feature, FeatureValue, LanguageFeature, DocumentLanguage
from database import get_genera, global_session, calculate_macroarea_distribution, get_macroarea_by_genus
from collections import Counter
from sqlalchemy.orm import joinedload

class SamplingResult:
    """
    Класс для представления результата сэмплинга.
    """
    def __init__(self, languages: List[Language], included_genera: List[Genus]):
        self.languages = languages
        self.included_genera = included_genera

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
        ranking_key: Optional[str] = None
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
            Будут выбраны только языки, имеющие документацию на этих языках.
        ranking_key : str, optional
            Критерий ранжирования языков при выборе из рода.
            Возможные значения: 'source_count', None (случайный выбор).
        """
        if genus_list is None:
            self.genus_list = global_session.query(Genus).all()
        else:
            self.genus_list = genus_list
        
        self.macroareas = macroareas
        self.include_languages = set(include_languages) if include_languages else set()
        self.exclude_languages = set(exclude_languages) if exclude_languages else set()
        self.wals_features = wals_features or {}
        self.grambank_features = grambank_features or {}
        self.doc_languages = set(doc_languages) if doc_languages else set()
        self.ranking_key = ranking_key
        
        super().__init__()


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

    def get_language_rank_score(self, language: Language) -> int:
        """
        Вычисляет оценку языка для ранжирования на основе выбранного критерия.
        
        Parameters
        ----------
        language : Language
            Язык для оценки.
            
        Returns
        -------
        int
            Оценка языка (чем больше, тем лучше). Для случайного выбора возвращает 0.
        """
        if self.ranking_key == 'source_count':
            # Количество библиографических источников
            from models import Source
            count = global_session.query(Source).filter_by(
                language_glottocode=language.glottocode
            ).count()
            return count
        else:
            # Без ранжирования - случайный выбор
            return 0

    def select_best_language(self, available_languages: List[Language]) -> Language:
        """
        Выбирает лучший язык из списка доступных на основе критерия ранжирования.
        
        Parameters
        ----------
        available_languages : List[Language]
            Список языков для выбора.
            
        Returns
        -------
        Language
            Выбранный язык. Если ранжирование не задано, выбирается случайный язык.
        """
        if not available_languages:
            return None
        
        if self.ranking_key:
            # Сортируем языки по убыванию оценки
            scored_languages = [(lang, self.get_language_rank_score(lang)) for lang in available_languages]
            # Сортируем по оценке (от большего к меньшему)
            scored_languages.sort(key=lambda x: x[1], reverse=True)
            # Возвращаем язык с максимальной оценкой
            return scored_languages[0][0]
        else:
            # Случайный выбор
            return random.choice(available_languages)

    def get_genus_rank_score(self, genus: Genus) -> int:
        """
        Вычисляет оценку рода для ранжирования на основе выбранного критерия.
        Оценка рода = максимальная оценка среди всех его языков, прошедших фильтры.
        
        Parameters
        ----------
        genus : Genus
            Род для оценки.
            
        Returns
        -------
        int
            Оценка рода (чем больше, тем лучше). Для случайного выбора возвращает 0.
        """
        if not self.ranking_key or not genus.languages:
            return 0
        
        # Применяем фильтры к языкам рода
        available_languages = self.apply_all_filters(genus.languages)
        if not available_languages:
            return 0
        
        # Находим максимальную оценку среди языков рода
        max_score = max(self.get_language_rank_score(lang) for lang in available_languages)
        return max_score

    def select_best_genera(self, available_genera: List[Genus], count: int) -> List[Genus]:
        """
        Выбирает лучшие роды из списка доступных на основе критерия ранжирования.
        
        Parameters
        ----------
        available_genera : List[Genus]
            Список родов для выбора.
        count : int
            Количество родов для выбора.
            
        Returns
        -------
        List[Genus]
            Список выбранных родов.
        """
        if not available_genera:
            return []
        
        count = min(count, len(available_genera))
        
        if self.ranking_key:
            # Сортируем роды по убыванию оценки
            scored_genera = [(genus, self.get_genus_rank_score(genus)) for genus in available_genera]
            # Сортируем по оценке (от большего к меньшему)
            scored_genera.sort(key=lambda x: x[1], reverse=True)
            # Возвращаем топ N родов
            return [genus for genus, score in scored_genera[:count]]
        else:
            # Случайный выбор
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
        
        filtered = []
        
        for lang in languages:
            # Получаем все фичи этого языка
            lang_features = global_session.query(LanguageFeature).filter_by(
                language_glottocode=lang.glottocode
            ).all()
            
            # Создаем словарь фича -> значение для быстрого поиска
            lang_feature_dict = {lf.feature_code: lf.value_code for lf in lang_features}
            
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
        
        filtered = []
        for lang in languages:
            # Получаем все языки документации для данного языка
            lang_doc_langs = global_session.query(DocumentLanguage).filter_by(
                language_glottocode=lang.glottocode
            ).all()
            
            # Проверяем, есть ли пересечение с требуемыми языками документации
            lang_doc_codes = {dl.doc_language_code for dl in lang_doc_langs}
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
        target_distribution = {
            macroarea: round(remaining_size * (count / total_genera))
            for macroarea, count in macroarea_distribution.items()
        }
        
        # Корректируем распределение, чтобы сумма была точно equal remaining_size
        current_total = sum(target_distribution.values())
        if current_total != remaining_size and target_distribution:
            # Находим макроареал с наибольшим количеством родов для корректировки
            largest_macroarea = max(macroarea_distribution.keys(), key=lambda m: macroarea_distribution[m])
            target_distribution[largest_macroarea] += (remaining_size - current_total)

        macroarea_genera = {area: [] for area in macroarea_distribution.keys()}
        for genus in self.genus_list:
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
        
        # Первый проход: распределяем по макроареалам согласно целевому распределению
        for macroarea, target_count in target_distribution.items():
            # Исключаем уже использованные роды
            available_genera = [g for g in macroarea_genera.get(macroarea, []) if g not in included_genera]
            num_to_select = min(target_count, len(available_genera))
            
            if num_to_select > 0:
                selected_genera = self.select_best_genera(available_genera, num_to_select)
                
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
        
        # Второй проход: добираем недостающие языки из любых доступных родов
        shortage = remaining_size - (len(selected_languages) - len(mandatory_languages))
        if shortage > 0:
            # Собираем все оставшиеся доступные роды из всех макроареалов
            all_remaining_genera = []
            for macroarea_genera_list in macroarea_genera.values():
                for genus in macroarea_genera_list:
                    if genus not in included_genera:
                        all_remaining_genera.append(genus)
            
            # Убираем дубликаты (род может быть в нескольких макроареалах)
            all_remaining_genera = list(set(all_remaining_genera))
            
            num_additional = min(shortage, len(all_remaining_genera))
            if num_additional > 0:
                additional_genera = self.select_best_genera(all_remaining_genera, num_additional)
                
                for genus in additional_genera:
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
        for genus in self.genus_list:
            if genus in mandatory_genera:
                continue
            
            # Проверяем, есть ли у рода языки после фильтрации
            if self.genus_has_available_languages(genus):
                available_genera.append(genus)
        
        # Выбираем нужное количество родов
        num_to_select = min(remaining_size, len(available_genera))
        
        selected_languages = list(mandatory_languages)
        included_genera = list(mandatory_genera)
        
        if num_to_select > 0:
            # Выбираем роды (с ранжированием или случайно)
            selected_genera = self.select_best_genera(available_genera, num_to_select)
            
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
    
    