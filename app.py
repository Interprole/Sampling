from flask import Flask, redirect, url_for, jsonify
from flask import render_template
from flask import request
from sqlalchemy import or_
from make_sample import GenusSample, preload_caches
from database import get_all_features, get_feature_values, get_all_document_language_codes, global_session
from models import Language

app = Flask(__name__)

# Предзагрузка кэшей при старте приложения
preload_caches()

def iso_to_glottocode(iso_codes):
    """
    Конвертирует список ISO 639-3 кодов в glottocodes.
    
    Parameters
    ----------
    iso_codes : List[str]
        Список ISO кодов.
        
    Returns
    -------
    List[str]
        Список glottocodes.
    """
    if not iso_codes:
        return []
    
    glottocodes = []
    for iso_code in iso_codes:
        lang = global_session.query(Language).filter_by(iso=iso_code).first()
        if lang:
            glottocodes.append(lang.glottocode)
    
    return glottocodes

@app.route("/")
def main():
    return render_template("main.html")


@app.route("/sample", methods=['POST'])
def sample():
    if request.method == "POST":
        # Получаем параметры из формы
        title = request.form.get('name', 'Untitled Sample')
        algorithm = request.form.get('sampling-algorithm', 'genus-macroarea')
        size = int(request.form.get('sample-size', 50))
        is_Spoken = request.form.get('isSpoken') is not None
        is_Sign = request.form.get('isSign') is not None
        
        # Списковые параметры
        macroareas = request.form.getlist('macroareas[]')
        docLang = request.form.getlist('docLanguages[]')
        rank = request.form.getlist('ranking')
        includeLang = request.form.getlist('include[]')
        excludeLang = request.form.getlist('exclude[]')
        documentTypes = request.form.getlist('documentTypes[]')
        
        # Конвертируем ISO коды в glottocodes для include/exclude
        includeLang_glottocodes = iso_to_glottocode(includeLang)
        excludeLang_glottocodes = iso_to_glottocode(excludeLang)
        
        # Фичи из Grambank и WALS (формат: feature_code:value_code)
        grambank_raw = request.form.getlist('grambank[]')
        wals_raw = request.form.getlist('wals[]')
        
        # Парсим фичи в словари
        grambank_features = {}
        for item in grambank_raw:
            if '-' in item:
                feature_code, value_code = item.split('-', 1)
                if feature_code not in grambank_features:
                    grambank_features[feature_code] = []
                grambank_features[feature_code].append(value_code)
        
        wals_features = {}
        for item in wals_raw:
            if '-' in item:
                feature_code, value_code = item.split('-', 1)
                # WALS значения обычно в формате "1.0", "2.0" и т.д.
                # Если передано просто "1", преобразуем в "1.0"
                if '.' not in value_code:
                    value_code = value_code + '.0'
                if feature_code not in wals_features:
                    wals_features[feature_code] = []
                wals_features[feature_code].append(value_code)
        
        # Определяем ключ ранжирования
        ranking_key = None
        if rank and len(rank) > 0:
            ranking_key = rank[0]  # Берем первое значение из списка
        
        # Фильтрация по модальности языков (spoken/sign)
        # Sign Languages имеют genus_id = 410
        SIGN_LANGUAGE_GENUS_ID = 410
        
        # Получаем список родов для фильтрации
        from models import Genus, Group
        from sqlalchemy.orm import joinedload
        filtered_genus_list = None
        
        if is_Spoken and is_Sign:
            # Оба включены - используем все роды
            filtered_genus_list = None
        elif is_Spoken and not is_Sign:
            # Только spoken - исключаем sign languages
            all_genera = global_session.query(Genus).options(
                joinedload(Genus.languages).joinedload(Group.macroarea)
            ).all()
            filtered_genus_list = [g for g in all_genera if g.id != SIGN_LANGUAGE_GENUS_ID]
        elif is_Sign and not is_Spoken:
            # Только sign - только sign languages
            filtered_genus_list = global_session.query(Genus).options(
                joinedload(Genus.languages).joinedload(Group.macroarea)
            ).filter_by(id=SIGN_LANGUAGE_GENUS_ID).all()
        else:
            # Ничего не выбрано - используем все роды
            filtered_genus_list = None
        
        # Создаем sampler с фильтрами
        sampler = GenusSample(
            genus_list=filtered_genus_list,
            macroareas=macroareas if macroareas else None,
            include_languages=includeLang_glottocodes if includeLang_glottocodes else None,
            exclude_languages=excludeLang_glottocodes if excludeLang_glottocodes else None,
            wals_features=wals_features if wals_features else None,
            grambank_features=grambank_features if grambank_features else None,
            doc_languages=docLang if docLang else None,
            ranking_key=ranking_key,
            document_types=documentTypes if documentTypes else None
        )
        
        # Выбираем алгоритм сэмплинга
        if algorithm == 'genus-macroarea':
            result = sampler.primary_sample(sample_size=size)
        elif algorithm == 'random':
            result = sampler.random_sample(sample_size=size)
        elif algorithm == 'diversity-value':
            result = sampler.diversity_value_sample(sample_size=size)
        else:
            return "Unknown sampling algorithm", 400
        
        # Получаем информацию о фичах для отображения
        from models import Feature, FeatureValue, LanguageFeature
        
        # Собираем все feature codes из фильтров
        all_feature_codes = list(wals_features.keys()) + list(grambank_features.keys())
        
        # Получаем полные данные о фичах
        feature_info = {}
        if all_feature_codes:
            for feature_code in all_feature_codes:
                feature = global_session.query(Feature).filter_by(code=feature_code).first()
                if feature:
                    # Определяем какие значения были в фильтре
                    if feature_code in wals_features:
                        filter_values = wals_features[feature_code]
                    else:
                        filter_values = grambank_features[feature_code]
                    
                    # Получаем названия значений
                    value_names = []
                    for value_code in filter_values:
                        fv = global_session.query(FeatureValue).filter_by(
                            feature_code=feature_code, 
                            value_code=value_code
                        ).first()
                        if fv:
                            value_names.append(fv.value_name)
                    
                    feature_info[feature_code] = {
                        'name': feature.name,
                        'source': feature.source,
                        'filter_values': filter_values,
                        'value_names': value_names
                    }
        
        # Получаем источники и языки документации для всех языков
        from models import Source
        
        # Группируем языки по макроареалам для отображения
        languages_by_macroarea = {}
        for lang in result.languages:
            macroarea_name = lang.macroarea.name if lang.macroarea else 'Unknown'
            if macroarea_name not in languages_by_macroarea:
                languages_by_macroarea[macroarea_name] = []
            
            # Получаем значения фичей для этого языка
            lang_features = {}
            for feature_code in all_feature_codes:
                lang_feature = global_session.query(LanguageFeature).filter_by(
                    language_glottocode=lang.glottocode,
                    feature_code=feature_code
                ).first()
                if lang_feature:
                    # Получаем название значения
                    fv = global_session.query(FeatureValue).filter_by(
                        feature_code=feature_code,
                        value_code=lang_feature.value_code
                    ).first()
                    lang_features[feature_code] = {
                        'value_code': lang_feature.value_code,
                        'value_name': fv.value_name if fv else lang_feature.value_code
                    }
            
            # Получаем список источников для языка, сортируем по году (от новых к старым)
            sources = global_session.query(Source).filter_by(
                language_glottocode=lang.glottocode
            ).order_by(Source.year.desc().nullslast()).all()
            
            # Формируем список источников с годом, страницами и языками документации
            source_list = []
            sources_languages = []
            for s in sources:
                source_str = s.title
                details = []
                if s.year:
                    details.append(str(s.year))
                if s.pages:
                    details.append(f"{s.pages}pp")
                
                # Добавляем языки документации для этого источника
                doc_names = []
                if s.doc_language_codes:
                    doc_codes = s.doc_language_codes.split(',')
                    for code in doc_codes:
                        lang_obj = global_session.query(Language).filter_by(iso=code).first()
                        if lang_obj:
                            doc_names.append(lang_obj.name)
                        else:
                            doc_names.append(code)
                    if doc_names:
                        details.append(f"in {', '.join(doc_names)}")
                
                if details:
                    source_str += f" ({', '.join(details)})"
                source_list.append(source_str)
                sources_languages.append(set(doc_names))
            
            # Получаем языки документации для этого языка (из всех источников)
            doc_lang_codes = set()
            for source in sources:
                if source.doc_language_codes:
                    doc_lang_codes.update(source.doc_language_codes.split(','))
            
            # Получаем полные названия языков документации
            doc_lang_names = []
            for code in doc_lang_codes:
                lang_obj = global_session.query(Language).filter_by(iso=code).first()
                if lang_obj:
                    doc_lang_names.append(f"{lang_obj.name} ({code})")
                else:
                    doc_lang_names.append(code)
            
            languages_by_macroarea[macroarea_name].append({
                'name': lang.name,
                'glottocode': lang.glottocode,
                'iso': lang.iso,
                'genus': lang.genus.name if lang.genus else 'Unknown',
                'family': lang.family if lang.family else 'Unknown',
                'latitude': lang.latitude,
                'longitude': lang.longitude,
                'features': lang_features,
                'sources': source_list,
                'sources_languages': sources_languages,
                'doc_languages': doc_lang_names
            })
        
        meta_data_json = {
            "Title": title,
            "Sampling algorithm": "The Genus-Macroarea sampling method" if algorithm == "genus-macroarea" else ("The Diversity Value sampling method" if algorithm == "diversity-value" else "A Random Sample"),
            "Included Macroareas": ', '.join(macroareas) if macroareas else "Africa, Australia, Eurasia, North America, Papunesia, South America",
            "Sample Size": str(size),
            "Filtered by Features": '; '.join([f"{info["name"]} ({feature_code}, {info["source"]}): {', '.join(info["value_names"])}" for feature_code, info in feature_info.items()]),
            "Included Languages": ', '.join(code_to_text(includeLang, "iso")),
            "Excluded Languages": ', '.join(code_to_text(excludeLang, "iso")),
            "Included Descriptions' Languages": ', '.join(code_to_text(docLang, "iso")),
            "Select Languages By": "Random" if ranking_key == 'random' else ("Extensiveness of description (2 * pages + 0.5 * year)" if ranking_key == 'descriptive_ranking' else ("Total number of Descriptions" if ranking_key == 'source_count' else ("Descriptions' publication year" if ranking_key == 'year_ranking' else "Descriptions' page count"))),
            "Document Types Filter": ', '.join(documentTypes) if documentTypes else "All document types"
        }

        sample_data_json = []
        for macroarea, languages in languages_by_macroarea.items():
            for language in languages:
                line = {
                    "Language": language["name"],
                    "Glottocode": language.get("glottocode", "-"),
                    "ISO": language.get("iso", "-"),
                    "Genus": language.get("genus", "-"),
                    "Family": language.get("family", "-"),
                    "Macroarea": macroarea,
                    "Latitude": language.get("latitude", "-"),
                    "Longitude": language.get("longitude", "-"),
                    "Sources": "; ".join(language.get("sources", [])[:25])
                }

                if bool(docLang):
                    line["Documentation Languages"] = "; ".join(language.get("doc_languages", [])[:50])
                
                for feature_code in all_feature_codes:
                    feature_name = language.get("features", {}).get(feature_code, {}).get("value_name", "")
                    line[feature_code] = feature_name
                
                sample_data_json.append(line)

        return render_template(
            "sample.html",
            title=title,
            algorithm=algorithm,
            sampleSize=size,
            macroareas=macroareas,
            docLang=code_to_text(docLang, "iso"),
            includeLang=code_to_text(includeLang, "iso"),
            excludeLang=code_to_text(excludeLang, "iso"),
            ranking_key=ranking_key,
            documentTypes=documentTypes,
            languages_by_macroarea=sorted(languages_by_macroarea.items()),
            total_languages=len(result.languages),
            total_genera=len(result.included_genera),
            feature_info=feature_info,
            all_feature_codes=all_feature_codes,
            has_doc_lang_filter=bool(docLang),
            target_macroarea_distribution=result.target_macroarea_distribution,
            actual_macroarea_distribution=result.actual_macroarea_distribution,
            meta_data_json=meta_data_json,
            sample_data_json=sample_data_json
        )


@app.route("/manual")
def manual():
    return render_template("manual.html")


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/api/features/wals")
def api_wals_features():
    """API endpoint для получения списка WALS фичей."""
    features = get_all_features(source='WALS')
    result = []
    for feature in features:
        values = get_feature_values(feature.code)
        result.append({
            'code': feature.code,
            'name': feature.name,
            'values': [{'code': v.value_code, 'name': v.value_name} for v in values]
        })
    return jsonify(result)


@app.route("/api/features/grambank")
def api_grambank_features():
    """API endpoint для получения списка Grambank фичей."""
    features = get_all_features(source='Grambank')
    result = []
    for feature in features:
        values = get_feature_values(feature.code)
        result.append({
            'code': feature.code,
            'name': feature.name,
            'values': [{'code': v.value_code, 'name': v.value_name} for v in values]
        })
    return jsonify(result)


@app.route("/api/document-languages")
def api_document_languages():
    """API endpoint для получения списка языков документации."""
    from models import Source, Language
    
    # Получаем параметр поиска
    search_term = request.args.get('q', '').strip().lower()
    
    # Получаем уникальные коды языков документации из всех источников
    sources = global_session.query(Source.doc_language_codes).filter(
        Source.doc_language_codes != None
    ).distinct().all()
    
    # Собираем все уникальные коды
    doc_lang_codes = set()
    for (codes_str,) in sources:
        if codes_str:
            doc_lang_codes.update(codes_str.split(','))
    
    result = []
    for lang_code in sorted(doc_lang_codes):
        # Ищем язык по ISO коду в таблице groups
        lang = global_session.query(Language).filter_by(iso=lang_code).first()
        
        if lang:
            # Если нашли - используем полное название
            text = f"{lang.name} ({lang_code})"
            # Фильтруем по поисковому запросу если он есть
            if not search_term or search_term in text.lower():
                result.append({
                    'id': lang_code,
                    'text': text
                })
        else:
            # Если не нашли - просто код
            if not search_term or search_term in lang_code.lower():
                result.append({
                    'id': lang_code,
                    'text': lang_code
                })
    
    return jsonify({'results': result})


@app.route("/api/languages")
def api_languages():
    """API endpoint для получения списка всех языков для include/exclude."""
    from models import Language
    
    # Получаем параметр поиска
    search_term = request.args.get('q', '').strip()
    
    # Базовый запрос
    query = global_session.query(Language).filter(Language.name.isnot(None))
    
    # Если есть поисковый запрос, фильтруем
    if search_term:
        # Поиск по имени языка (case-insensitive)
        search_pattern = f"%{search_term}%"
        query = query.filter(
            or_(
                Language.name.ilike(search_pattern),
                Language.iso.ilike(search_pattern),
                Language.glottocode.ilike(search_pattern)
            )
        )
    
    # order_by ПЕРЕД limit
    query = query.order_by(Language.name).limit(50)
    
    languages = query.all()
    
    result = []
    for lang in languages:
        result.append({
            'id': lang.iso if lang.iso else lang.glottocode,
            'text': lang.name
        })
    
    return jsonify({'results': result})


@app.route("/api/macroareas")
def api_macroareas():
    """API endpoint для получения списка макроареалов."""
    from models import Macroarea
    
    # Получаем параметр поиска
    search_term = request.args.get('q', '').strip().lower()
    
    macroareas = global_session.query(Macroarea).order_by(Macroarea.name).all()
    
    result = []
    for ma in macroareas:
        # Фильтруем по поисковому запросу если он есть
        if not search_term or search_term in ma.name.lower():
            result.append({
                'id': ma.name,
                'text': ma.name
            })
    
    return jsonify({'results': result})


@app.route("/api/sources")
def api_sources():
    """API endpoint для получения списка источников."""
    from models import Source
    
    # Получаем параметр поиска
    search_term = request.args.get('q', '').strip().lower()
    
    # Получаем уникальные источники
    sources = global_session.query(Source.source).distinct().order_by(Source.source).all()
    sources = [s[0] for s in sources]
    
    result = []
    for source in sources:
        # Фильтруем по поисковому запросу если он есть
        if not search_term or search_term in source.lower():
            result.append({
                'id': source,
                'text': source
            })
    
    # Ограничиваем до 50 результатов для производительности
    return jsonify({'results': result[:50]})


@app.route("/api/document-types")
def api_document_types():
    """API endpoint для получения списка типов документов (grammar, dictionary, grammar_sketch, etc.)."""
    from models import Source
    
    # Получаем параметр поиска
    search_term = request.args.get('q', '').strip().lower()
    
    # Получаем уникальные типы документов
    doc_types = global_session.query(Source.document_type).filter(
        Source.document_type != None,
        Source.document_type != ''
    ).distinct().all()
    
    # Собираем уникальные типы (некоторые могут быть через запятую)
    unique_types = set()
    for (doc_type,) in doc_types:
        if doc_type:
            # Разбиваем по запятой если есть несколько типов
            for t in doc_type.split(','):
                t = t.strip()
                if t:
                    unique_types.add(t)
    
    # Сортируем и фильтруем
    result = []
    for doc_type in sorted(unique_types):
        if not search_term or search_term in doc_type.lower():
            result.append({
                'id': doc_type,
                'text': doc_type
            })
    
    return jsonify({'results': result})


def code_to_text(codes, mode):
    languageNames = set()
    for code in codes:
        if mode == "iso":
            language = global_session.query(Language).filter_by(iso=code).first()
        if mode == "glottocode":
            language = global_session.query(Language).filter_by(glottocode=code).first()
        if language:
            languageNames.add(language.name)
    return languageNames


if __name__ == "__main__":
    app.run(debug=True)
