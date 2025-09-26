import random
from typing import List, Dict, Callable
from models import Language, Genus, Macroarea
from database import get_genera, global_session, calculate_macroarea_distribution, get_macroarea_by_genus
from collections import Counter

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
    def __init__(self, genus_list: List[Genus] = None):
        if genus_list is None:
            self.genus_list = global_session.query(Genus).all()
        else:
            self.genus_list = genus_list
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

    def genus_sample(self) -> SamplingResult:
        """
        Genus Sample (GS): One language from every genus.
        """
        selected_languages = []
        included_genera = []
        for genus in self.genus_list:
            if genus.languages:
                selected_languages.append(random.choice(genus.languages))
                included_genera.append(genus)
        return SamplingResult(selected_languages, included_genera)

    def core_sample(self) -> SamplingResult:
        """
        Core Sample (CS): One language from every genus with usable sources of data.
        """
        selected_languages = []
        included_genera = []
        for genus in self.genus_list:
            #usable_languages = [lang for lang in genus.languages if lang.usable_sources]
            usable_languages = [lang for lang in genus.languages]

            if usable_languages:
                selected_languages.append(random.choice(usable_languages))
                included_genera.append(genus)
        return SamplingResult(selected_languages, included_genera)

    def restricted_sample(self) -> SamplingResult:
        """
        Restricted Sample (RS): Subsample of CS with the same genealogical diversity distribution as macroarea_distribution.
        """
        macroarea_distribution = calculate_macroarea_distribution()
        total_genera = sum(macroarea_distribution.values())
        cs_result = self.core_sample()

        # Calculate the target number of genera for each macroarea based on proportions
        target_distribution = {
            macroarea: round(len(cs_result.included_genera) * (count / total_genera))
            for macroarea, count in macroarea_distribution.items()
        }

        macroarea_genera = {area: [] for area in macroarea_distribution.keys()}
        for genus in cs_result.included_genera:
            macroareas = get_macroarea_by_genus(genus)
            for macroarea in macroareas:
                macroarea_genera[macroarea].append(genus)

        selected_languages = []
        included_genera = []
        for macroarea, target_count in target_distribution.items():
            selected_genera = macroarea_genera.get(macroarea, [])[:target_count]
            for genus in selected_genera:
                if genus.languages:
                    selected_languages.append(random.choice(genus.languages))
                    included_genera.append(genus)

        return SamplingResult(selected_languages, included_genera)

    def primary_sample(self, sample_size: int) -> SamplingResult:
        """
        Primary Sample (PS): Predetermined sample size with equal genealogical diversity in each macroarea.
        """
        macroarea_distribution = calculate_macroarea_distribution()
        total_genera = sum(macroarea_distribution.values())

        # Calculate the target number of genera for each macroarea based on proportions
        target_distribution = {
            macroarea: round(sample_size * (count / total_genera))
            for macroarea, count in macroarea_distribution.items()
        }

        macroarea_genera = {area: [] for area in macroarea_distribution.keys()}
        for genus in self.genus_list:
            macroareas = get_macroarea_by_genus(genus)
            for macroarea in macroareas:
                macroarea_genera[macroarea].append(genus)

        selected_languages = []
        included_genera = []
        for macroarea, target_count in target_distribution.items():
            selected_genera = random.sample(macroarea_genera.get(macroarea, []), min(target_count, len(macroarea_genera.get(macroarea, []))))
            for genus in selected_genera:
                if genus.languages:
                    selected_languages.append(random.choice(genus.languages))
                    included_genera.append(genus)

        return SamplingResult(selected_languages, included_genera)
    
    