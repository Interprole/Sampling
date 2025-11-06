import csv
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import (Base, Genus, Macroarea, Group,
                    create_tables)
from tqdm import tqdm

def load_genera_data(session, filepath):
    """Load genera data from TSV file into database"""
    print(f"Loading genera data from {filepath}")
    
    # Dictionary to keep track of genera we've already added
    genera_dict = {}
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        rows = list(reader)
        
        for row in tqdm(rows, desc="Loading Genera Data"):
            glottocode = row['glottocode']
            genus_name = row['genus']
            confidence = row['confidence']
            
            # Get or create genus
            if genus_name and genus_name != 'isolate':
                if genus_name not in genera_dict:
                    genus = Genus(name=genus_name)
                    session.add(genus)
                    session.flush()  # To get the ID
                    genera_dict[genus_name] = genus.id
                genus_id = genera_dict[genus_name]
                
                # Create language entry
                language = Group(
                    is_language=True,
                    is_genus=False,
                    glottocode=glottocode,
                    genus_id=genus_id,
                    genus_confidence=confidence
                )
            else:
                language = Group(
                    is_language=True,
                    is_genus=True,
                    glottocode=glottocode
                )

            session.add(language)

def load_macroareas_data(session, tsv_filepath):
    """Load macroareas data from TSV file into database"""
    # This function will be implemented when data is available
    try:
        if os.path.exists(tsv_filepath) and os.path.getsize(tsv_filepath) > 0:
            print(f"Loading macroareas from {tsv_filepath}")
            with open(tsv_filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                rows = list(reader)
                macroarea_dict = {}
                
                for row in tqdm(rows, desc="Loading Macroareas Data"):
                    # Add macroarea to database
                    macroarea_name = row['macroareas']
                    if macroarea_name:
                        if macroarea_name not in macroarea_dict:
                            macroarea = session.query(Macroarea).filter_by(name=macroarea_name).first()
                            if not macroarea:
                                # Create new macroarea
                                macroarea = Macroarea(name=macroarea_name)
                                session.add(macroarea)
                                session.flush()  # To get the ID
                            macroarea_dict[macroarea_name] = macroarea.id
                        
                        macroarea_id = macroarea_dict[macroarea_name]
                    else:
                        macroarea_id = None
                    
                    # Add groups to database
                    path = row['path']
                    groups = path.split('/')
                    
                    # Убираем 'tree' если он есть в начале
                    if groups[0] == 'tree':
                        groups = groups[1:]
                    
                    parent_code = None  # Первый элемент не имеет родителя
                    
                    for i, group_code in enumerate(groups):
                        # Проверяем, существует ли группа
                        group = session.query(Group).filter_by(glottocode=group_code).first()
                        
                        if not group:
                            # Создаем новую группу
                            group = Group(
                                glottocode=group_code,
                                closest_supergroup=parent_code,
                                is_genus=False,
                                is_language=(i == len(groups) - 1)  # Последний элемент - это язык
                            )
                            session.add(group)
                            session.flush()
                        else:
                            # Обновляем родителя, если его еще нет
                            if parent_code and not group.closest_supergroup:
                                group.closest_supergroup = parent_code
                        
                        # Текущая группа становится родителем для следующей
                        parent_code = group_code

                    # Update language entry
                    language = session.query(Group).filter_by(glottocode=row['glottocode'],
                                                                is_language=1).first()
                    if language:
                        if not language.genus_id:
                            genus = Genus(name=row['name'])
                            session.add(genus)
                            session.flush()
                            language.genus_id = genus.id
                            language.is_genus = True
                        language.macroarea_id = macroarea_id
                        language.latitude = row['latitude']
                        language.longitude = row['longitude']
                        language.iso= row['hid']
                        language.name = row['name']

        else:
            print("No macroarea data files found or files are empty.")
    except Exception as e:
        print(f"Error loading macroarea data: {e}")


def main():
    # Create database engine
    engine = create_engine('sqlite:///sql.db', echo=True)
    create_tables(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Define file paths
        data_dir = "data/"
        genera_tsv = os.path.join(data_dir, "genera.tsv")
        macroareas_tsv = os.path.join(data_dir, "macroareas.tsv")
        
        # Load data
        load_genera_data(session, genera_tsv)
        load_macroareas_data(session, macroareas_tsv)
        
        # Commit changes
        session.commit()
        print("Data import completed successfully!")
        
    except Exception as e:
        session.rollback()
        print(f"Error importing data: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
