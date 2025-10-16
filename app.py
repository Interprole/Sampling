from flask import Flask, redirect, url_for, jsonify
from flask import render_template
from flask import request
from make_sample import GenusSample
from database import get_all_features, get_feature_values, get_all_document_language_codes, global_session
from models import Language

app = Flask(__name__)

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
        
        # Списковые параметры
        macroareas = request.form.getlist('macroareas[]')
        docLang = request.form.getlist('docLanguages[]')
        rank = request.form.getlist('ranking')
        includeLang = request.form.getlist('include[]')
        excludeLang = request.form.getlist('exclude[]')
        
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
                if feature_code not in wals_features:
                    wals_features[feature_code] = []
                wals_features[feature_code].append(value_code)
        
        # Создаем sampler с фильтрами
        sampler = GenusSample(
            macroareas=macroareas if macroareas else None,
            include_languages=includeLang_glottocodes if includeLang_glottocodes else None,
            exclude_languages=excludeLang_glottocodes if excludeLang_glottocodes else None,
            wals_features=wals_features if wals_features else None,
            grambank_features=grambank_features if grambank_features else None,
            doc_languages=docLang if docLang else None
        )
        
        # Выбираем алгоритм сэмплинга
        if algorithm == 'genus-macroarea':
            result = sampler.primary_sample(sample_size=size)
        else:
            return "Unknown sampling algorithm", 400
        
        # Группируем языки по макроареалам для отображения
        languages_by_macroarea = {}
        for lang in result.languages:
            macroarea_name = lang.macroarea.name if lang.macroarea else 'Unknown'
            if macroarea_name not in languages_by_macroarea:
                languages_by_macroarea[macroarea_name] = []
            languages_by_macroarea[macroarea_name].append({
                'name': lang.name,
                'glottocode': lang.glottocode,
                'iso': lang.iso,
                'genus': lang.genus.name if lang.genus else 'Unknown',
                'latitude': lang.latitude,
                'longitude': lang.longitude
            })
        
        return render_template(
            "sample.html",
            title=title,
            languages_by_macroarea=languages_by_macroarea,
            total_languages=len(result.languages),
            total_genera=len(result.included_genera)
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
    doc_langs = get_all_document_language_codes()
    return jsonify(sorted(doc_langs))


if __name__ == "__main__":
    app.run(debug=True)
