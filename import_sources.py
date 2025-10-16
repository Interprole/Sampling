"""
Импорт источников из walsDataMerged.csv в таблицу sources.

Источники в CSV разделены пробелами и находятся в колонке 'Source'.
"""

import pandas as pd
from database import global_session
from models import Source, Language

def import_sources():
    """Импортирует источники из walsDataMerged.csv в базу данных."""
    
    # Читаем CSV
    df = pd.read_csv('data/walsDataMerged.csv')
    
    print(f"Loaded {len(df)} rows from walsDataMerged.csv")
    
    # Счетчики
    total_sources = 0
    languages_with_sources = 0
    skipped_no_glottocode = 0
    skipped_no_source = 0
    
    # Очищаем таблицу sources перед импортом
    global_session.query(Source).delete()
    global_session.commit()
    print("Cleared existing sources")
    
    for idx, row in df.iterrows():
        glottocode = row.get('Glottocode')
        source_str = row.get('Source')
        
        # Пропускаем если нет glottocode
        if pd.isna(glottocode) or not glottocode:
            skipped_no_glottocode += 1
            continue
        
        # Пропускаем если нет источников
        if pd.isna(source_str) or not source_str or source_str.strip() == '':
            skipped_no_source += 1
            continue
        
        # Разбиваем источники по пробелам
        sources = source_str.strip().split()
        
        for source in sources:
            source = source.strip()
            if source:
                # Добавляем источник в базу
                source_obj = Source(
                    language_glottocode=glottocode,
                    source=source
                )
                global_session.merge(source_obj)  # merge вместо add для избежания дубликатов
                total_sources += 1
        
        if sources:
            languages_with_sources += 1
        
        # Коммитим каждые 100 записей
        if idx % 100 == 0:
            global_session.commit()
            print(f"Processed {idx} rows...")
    
    # Финальный commit
    global_session.commit()
    
    print(f"\n=== Import Complete ===")
    print(f"Total languages in CSV: {len(df)}")
    print(f"Languages with sources: {languages_with_sources}")
    print(f"Total sources imported: {total_sources}")
    print(f"Skipped (no glottocode): {skipped_no_glottocode}")
    print(f"Skipped (no source): {skipped_no_source}")
    
    # Проверка
    count = global_session.query(Source).count()
    print(f"\nSources in database: {count}")
    
    # Примеры
    examples = global_session.query(Source).limit(10).all()
    print("\nFirst 10 sources:")
    for src in examples:
        print(f"  {src.language_glottocode}: {src.source}")


if __name__ == '__main__':
    import_sources()
