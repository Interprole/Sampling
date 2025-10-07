"""
Script to import WALS and Grambank features into the database.

This script:
1. Reads feature descriptions from JSON files
2. Reads language feature values from CSV files
3. Populates the database with Features, FeatureValues, and LanguageFeatures
"""

import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Feature, FeatureValue, LanguageFeature, Group, DocumentLanguage
import os


def load_json(filepath):
    """Load JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def import_wals_features(session, description_file, data_file):
    """
    Import WALS features into the database.
    
    Parameters
    ----------
    session : Session
        SQLAlchemy session
    description_file : str
        Path to walsFeatureDescription.json
    data_file : str
        Path to walsDataMerged.csv
    """
    print("Importing WALS features...")
    
    # Load feature descriptions
    descriptions = load_json(description_file)
    
    # Read CSV data
    df = pd.read_csv(data_file, low_memory=False)
    
    # Get all WALS feature columns (they start with numbers like "1A_", "2A_", etc.)
    wals_columns = [col for col in df.columns if col.split('_')[0][0].isdigit() if '_' in col]
    
    feature_count = 0
    value_count = 0
    language_feature_count = 0
    
    for feature_code in wals_columns:
        # Split feature code and name
        # Format: "1A_Consonant Inventories" -> code="1A", name="Consonant Inventories"
        parts = feature_code.split('_', 1)
        code = parts[0]
        name = parts[1] if len(parts) > 1 else code
        
        # Check if feature already exists
        existing_feature = session.query(Feature).filter_by(code=code).first()
        if existing_feature:
            print(f"Feature {code} already exists, skipping...")
            continue
        
        # Create feature
        feature = Feature(
            code=code,
            name=name,
            source='WALS',
            description=None
        )
        session.add(feature)
        session.flush()
        feature_count += 1
        
        # Add feature values from description file
        if feature_code in descriptions:
            value_descriptions = descriptions[feature_code]
            for value_code, value_name in value_descriptions.items():
                feature_value = FeatureValue(
                    feature_code=code,
                    value_code=value_code,
                    value_name=value_name
                )
                session.add(feature_value)
                value_count += 1
        
        # Import language-feature values
        for idx, row in df.iterrows():
            glottocode = row.get('Glottocode')
            feature_value = row.get(feature_code)
            
            if pd.notna(glottocode) and pd.notna(feature_value):
                # Check if language exists in database
                language = session.query(Group).filter_by(glottocode=glottocode).first()
                if language:
                    # Use merge to handle duplicates
                    language_feature = LanguageFeature(
                        language_glottocode=glottocode,
                        feature_code=code,
                        value_code=str(feature_value)
                    )
                    session.merge(language_feature)
                    language_feature_count += 1
        
        # Commit after each feature to avoid memory issues
        if feature_count % 10 == 0:
            session.commit()
            print(f"Processed {feature_count} WALS features...")
    
    session.commit()
    print(f"WALS import complete: {feature_count} features, {value_count} values, {language_feature_count} language-feature pairs")


def import_grambank_features(session, description_file, data_file):
    """
    Import Grambank features into the database.
    
    Parameters
    ----------
    session : Session
        SQLAlchemy session
    description_file : str
        Path to grambankFeatureDescription.json
    data_file : str
        Path to grambankDataMerged.csv
    """
    print("Importing Grambank features...")
    
    # Load feature descriptions
    descriptions = load_json(description_file)
    
    # Read CSV data
    df = pd.read_csv(data_file, low_memory=False)
    
    # Get all Grambank feature columns (they start with "GB")
    grambank_columns = [col for col in df.columns if col.startswith('GB')]
    
    feature_count = 0
    value_count = 0
    language_feature_count = 0
    
    for feature_code in grambank_columns:
        # For Grambank, code is already clean (e.g., "GB020")
        # But the full column name may include description
        # Format: "GB020_Are there definite or specific articles?" -> code="GB020", name from description
        parts = feature_code.split('_', 1)
        code = parts[0]
        name = parts[1] if len(parts) > 1 else code
        
        # Check if feature already exists
        existing_feature = session.query(Feature).filter_by(code=code).first()
        if existing_feature:
            print(f"Feature {code} already exists, skipping...")
            continue
        
        # Create feature
        feature = Feature(
            code=code,
            name=name,
            source='Grambank',
            description=None
        )
        session.add(feature)
        session.flush()
        feature_count += 1
        
        # Add feature values from description file
        if feature_code in descriptions:
            value_descriptions = descriptions[feature_code]
            for value_code, value_name in value_descriptions.items():
                feature_value = FeatureValue(
                    feature_code=code,
                    value_code=value_code,
                    value_name=value_name
                )
                session.add(feature_value)
                value_count += 1
        
        # Import language-feature values
        for idx, row in df.iterrows():
            glottocode = row.get('Glottocode')
            feature_value = row.get(feature_code)
            
            if pd.notna(glottocode) and pd.notna(feature_value):
                # Check if language exists in database
                language = session.query(Group).filter_by(glottocode=glottocode).first()
                if language:
                    # Convert value to string, handling integers properly
                    value_str = str(int(feature_value)) if isinstance(feature_value, float) and feature_value.is_integer() else str(feature_value)
                    
                    # Use merge to handle duplicates
                    language_feature = LanguageFeature(
                        language_glottocode=glottocode,
                        feature_code=code,
                        value_code=value_str
                    )
                    session.merge(language_feature)
                    language_feature_count += 1
        
        # Commit after each feature to avoid memory issues
        if feature_count % 10 == 0:
            session.commit()
            print(f"Processed {feature_count} Grambank features...")
    
    session.commit()
    print(f"Grambank import complete: {feature_count} features, {value_count} values, {language_feature_count} language-feature pairs")


def import_document_languages(session, data_file):
    """
    Import document languages (Sources' Languages) from WALS data.
    
    Parameters
    ----------
    session : Session
        SQLAlchemy session
    data_file : str
        Path to walsDataMerged.csv
    """
    print("Importing document languages...")
    
    # Read CSV data
    df = pd.read_csv(data_file, low_memory=False)
    
    # Check if column exists
    if "Sources' Languages" not in df.columns:
        print("Column 'Sources' Languages' not found in CSV")
        return
    
    doc_lang_count = 0
    
    for idx, row in df.iterrows():
        glottocode = row.get('Glottocode')
        sources_langs = row.get("Sources' Languages")
        
        if pd.notna(glottocode) and pd.notna(sources_langs):
            # Check if language exists in database
            language = session.query(Group).filter_by(glottocode=glottocode).first()
            if language:
                # Sources' Languages can be space-separated (e.g., "eng deu") or comma-separated
                # First try splitting by comma, if that gives one result, split by space
                if ',' in str(sources_langs):
                    doc_lang_codes = [lang.strip() for lang in str(sources_langs).split(',')]
                else:
                    doc_lang_codes = [lang.strip() for lang in str(sources_langs).split()]
                
                for doc_lang_code in doc_lang_codes:
                    if doc_lang_code:  # Skip empty strings
                        # Check if this pair already exists
                        existing = session.query(DocumentLanguage).filter_by(
                            language_glottocode=glottocode,
                            doc_language_code=doc_lang_code
                        ).first()
                        
                        if not existing:
                            doc_lang = DocumentLanguage(
                                language_glottocode=glottocode,
                                doc_language_code=doc_lang_code
                            )
                            session.add(doc_lang)
                            doc_lang_count += 1
        
        # Commit periodically
        if idx % 500 == 0:
            session.commit()
            print(f"Processed {idx} rows...")
    
    session.commit()
    print(f"Document languages import complete: {doc_lang_count} language-document_language pairs")


def main():
    """Main function to run the import process."""
    # Database setup
    engine = create_engine('sqlite:///sql.db')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Define file paths
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, 'data')
        
        wals_description = os.path.join(data_dir, 'walsFeatureDescription.json')
        wals_data = os.path.join(data_dir, 'walsDataMerged.csv')
        
        grambank_description = os.path.join(data_dir, 'grambankFeatureDescription.json')
        grambank_data = os.path.join(data_dir, 'grambankDataMerged.csv')
        
        # Import WALS features
        if os.path.exists(wals_description) and os.path.exists(wals_data):
            import_wals_features(session, wals_description, wals_data)
            # Import document languages from WALS data
            import_document_languages(session, wals_data)
        else:
            print(f"WALS files not found:")
            print(f"  Description: {wals_description}")
            print(f"  Data: {wals_data}")
        
        # Import Grambank features
        if os.path.exists(grambank_description) and os.path.exists(grambank_data):
            import_grambank_features(session, grambank_description, grambank_data)
        else:
            print(f"Grambank files not found:")
            print(f"  Description: {grambank_description}")
            print(f"  Data: {grambank_data}")
        
        print("\nImport process completed successfully!")
        
        # Print summary statistics
        feature_count = session.query(Feature).count()
        value_count = session.query(FeatureValue).count()
        language_feature_count = session.query(LanguageFeature).count()
        doc_lang_count = session.query(DocumentLanguage).count()
        
        print(f"\nDatabase summary:")
        print(f"  Total features: {feature_count}")
        print(f"  Total feature values: {value_count}")
        print(f"  Total language-feature pairs: {language_feature_count}")
        print(f"  Total language-document_language pairs: {doc_lang_count}")
        
    except Exception as e:
        print(f"Error during import: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    main()
