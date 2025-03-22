import random
from typing import List, Dict
from models import Language, Genus, Macroarea

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
            "genera_coverage": {genus.name: [lang.name for lang in genus.languages] for genus in self.included_genera}
        }


class Sample:
    """
    Базовый класс для сэмплов языков.
    """
    def __init__(self, sampling_result: SamplingResult):
        self.sampling_result = sampling_result

    def __str__(self) -> str:
        return f"{self.__class__.__name__} with {len(self.sampling_result.languages)} languages"

    def __iter__(self):
        return iter(self.sampling_result.languages)

    def __len__(self):
        return len(self.sampling_result.languages)

    def __getitem__(self, index):
        return self.sampling_result.languages[index]


class GenusSample(Sample):
    """
    Базовый класс для сэмплов на уровне родов (genus).
    """
    def __init__(self, genus_list: List[Genus]):
        self.genus_list = genus_list
        sampling_result = self.create_sample()
        super().__init__(sampling_result)

    def create_sample(self) -> SamplingResult:
        """
        Метод для создания выборки. Должен быть переопределён в подклассах.
        """
        raise NotImplementedError

    def get_languages_from_genus(self, genus: Genus) -> List[Language]:
        """
        Вспомогательный метод, возвращающий список языков в данном роде.
        """
        return genus.languages if genus.languages else []


# --- 1. Случайный выбор языка из каждого рода ---
class RandomGenusSample(GenusSample):
    """
    Выбирает случайный язык из каждого рода.
    """
    def create_sample(self) -> SamplingResult:
        selected_languages = []
        included_genera = []
        for genus in self.genus_list:
            if genus.languages:
                selected_languages.append(random.choice(genus.languages))
                included_genera.append(genus)
        return SamplingResult(selected_languages, included_genera)


# --- 2. Выбор самого хорошо документированного языка ---
class MostDocumentedGenusSample(GenusSample):
    """
    Выбирает язык с наибольшим количеством доступных источников из каждого рода.
    """
    def create_sample(self) -> SamplingResult:
        selected_languages = []
        included_genera = []
        for genus in self.genus_list:
            if genus.languages:
                best_language = max(genus.languages, key=lambda lang: lang.documentation_score)
                selected_languages.append(best_language)
                included_genera.append(genus)
        return SamplingResult(selected_languages, included_genera)


# --- 3. Сбалансированный выбор языка с учётом ареального разнообразия ---
class BalancedGenusSample(GenusSample):
    """
    Выбирает языки так, чтобы сбалансировать представительство макрообластей.
    
    :param macroarea_distribution: Словарь {название макрообласти: желаемое количество языков}.
    """
    def __init__(self, genus_list: List[Genus], macroarea_distribution: Dict[str, int]):
        self.macroarea_distribution = macroarea_distribution
        self.macroarea_counts = {area: 0 for area in macroarea_distribution.keys()}
        super().__init__(genus_list)

    def create_sample(self) -> SamplingResult:
        selected_languages = []
        included_genera = []
        
        for genus in self.genus_list:
            if genus.languages:
                # Отбираем языки, которые соответствуют ограничениям макрообластей
                filtered_languages = [
                    lang for lang in self.get_languages_from_genus(genus)
                    if self.macroarea_counts.get(lang.macroarea, 0) < self.macroarea_distribution.get(lang.macroarea, 0)
                ]

                if filtered_languages:
                    chosen_language = random.choice(filtered_languages)
                    self.macroarea_counts[chosen_language.macroarea] += 1
                else:
                    # Если в макрообласти достигнут лимит, берём любой язык из рода
                    chosen_language = random.choice(genus.languages)

                selected_languages.append(chosen_language)
                included_genera.append(genus)

        return SamplingResult(selected_languages, included_genera)
