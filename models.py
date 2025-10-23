from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class Genus(Base):
    """
    A class used to represent a Genus.

    Attributes
    ----------
    name : str
        The name of the genus.
    """
    __tablename__ = 'genera'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    languages = relationship("Group", back_populates="genus")
    

class Macroarea(Base):
    """
    A class used to represent a Macroarea.

    Attributes
    ----------
    id : int
        The unique identifier of the macroarea.
    name : str
        The name of the macroarea.
    """
    __tablename__ = 'macroareas'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    languages = relationship("Group", back_populates="macroarea")

class Group(Base):
    """
    A class used to represent a Group.

    Attributes
    ----------
    name : str
        The name of the group.
    closest_supergroup : str
        The name of the closest supergroup.
    is_genus : bool
        Indicates if the group is a genus.
    """
    __tablename__ = 'groups'
    
    # Add discriminator column for inheritance
    type = Column(String(50))
    
    glottocode = Column(String, primary_key=True)
    iso = Column(String)
    name = Column(String)

    is_genus = Column(Boolean, default=False, nullable=False)

    is_language = Column(Boolean, default=False, nullable=False)
    genus_id = Column(Integer, ForeignKey('genera.id'))
    genus = relationship("Genus", back_populates="languages")
    genus_confidence = Column(String)
    latitude = Column(String)
    longitude = Column(String)
    macroarea_id = Column(Integer, ForeignKey('macroareas.id'))
    macroarea = relationship("Macroarea", back_populates="languages")
    
    closest_supergroup = Column(String, ForeignKey('groups.glottocode'))
    subgroups = relationship("Group", remote_side=[glottocode], backref="supergroup")
    
    __mapper_args__ = {
        'polymorphic_identity': 'group',
        'polymorphic_on': type
    }


class Language(Group):
    """
    A class representing a Language, which is a specialized Group.
    
    All Language instances are language nodes in Glottolog, with specific attributes
    like ISO codes, coordinates, and genus membership.
    """
    
    __mapper_args__ = {
        'polymorphic_identity': 'language',
        'polymorphic_on': Group.type
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_language = True


class Feature(Base):
    """
    A class used to represent a linguistic Feature from WALS or Grambank.
    
    Attributes
    ----------
    code : str
        The feature code (e.g., '1A', 'GB020') - primary key
    name : str
        The full name of the feature (e.g., 'Consonant Inventories')
    source : str
        The source database ('WALS' or 'Grambank')
    description : str
        Optional description of the feature
    """
    __tablename__ = 'features'
    
    code = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    source = Column(String, nullable=False)  # 'WALS' or 'Grambank'
    description = Column(String)
    
    # Relationships
    values = relationship("FeatureValue", back_populates="feature", cascade="all, delete-orphan")
    language_features = relationship("LanguageFeature", back_populates="feature", cascade="all, delete-orphan")


class FeatureValue(Base):
    """
    A class used to represent possible values for a Feature.
    
    Attributes
    ----------
    feature_code : str
        Foreign key to Feature.code
    value_code : str
        The code for this value (e.g., '1.0', '2.0', '0', '1')
    value_name : str
        The name/description of this value
    """
    __tablename__ = 'feature_values'
    
    feature_code = Column(String, ForeignKey('features.code'), primary_key=True)
    value_code = Column(String, primary_key=True)
    value_name = Column(String, nullable=False)
    
    # Relationships
    feature = relationship("Feature", back_populates="values")


class LanguageFeature(Base):
    """
    A class used to represent a Feature value for a specific Language.
    
    Attributes
    ----------
    language_glottocode : str
        Foreign key to Group (Language)
    feature_code : str
        Foreign key to Feature.code
    value_code : str
        The value code for this language-feature pair
    """
    __tablename__ = 'language_features'
    
    language_glottocode = Column(String, ForeignKey('groups.glottocode'), primary_key=True)
    feature_code = Column(String, ForeignKey('features.code'), primary_key=True)
    value_code = Column(String, nullable=False)
    
    # Relationships
    language = relationship("Group", foreign_keys=[language_glottocode])
    feature = relationship("Feature", back_populates="language_features")


class Source(Base):
    """
    A class used to represent a bibliographic source for a language.
    
    Attributes
    ----------
    language_glottocode : str
        Foreign key to Group (Language)
    title : str
        The full title of the source
    year : int
        Publication year (nullable)
    pages : int
        Number of pages (nullable)
    document_type : str
        Type of document (e.g., 'grammar', 'dictionary', 'grammar_sketch', etc.)
    doc_language_codes : str
        Comma-separated ISO 639-3 codes of documentation languages (e.g., 'eng,rus,fra')
    """
    __tablename__ = 'sources'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    language_glottocode = Column(String, ForeignKey('groups.glottocode'), nullable=False)
    title = Column(String, nullable=False)
    year = Column(Integer)  # Can be None
    pages = Column(Integer)  # Can be None
    document_type = Column(String)  # Can be None or comma-separated types
    doc_language_codes = Column(String)  # Comma-separated codes: 'eng,rus,fra'
    
    # Relationships
    language = relationship("Group", foreign_keys=[language_glottocode])


class LanguageRankingCache(Base):
    """
    A class used to cache ranking scores for languages.
    Speeds up sampling by avoiding repeated calculations.
    
    Attributes
    ----------
    language_glottocode : str
        Foreign key to Group (Language)
    source_count : int
        Total number of sources for this language
    max_year : int
        Maximum publication year among sources
    max_pages : int
        Maximum page count among sources
    grammar_count : int
        Number of grammar sources
    dictionary_count : int
        Number of dictionary sources
    last_updated : int
        Timestamp of last cache update (Unix timestamp)
    """
    __tablename__ = 'language_ranking_cache'
    
    language_glottocode = Column(String, ForeignKey('groups.glottocode'), primary_key=True)
    source_count = Column(Integer, default=0)
    max_year = Column(Integer, default=0)
    max_pages = Column(Integer, default=0)
    grammar_count = Column(Integer, default=0)
    dictionary_count = Column(Integer, default=0)
    last_updated = Column(Integer, default=0)  # Unix timestamp
    
    # Relationships
    language = relationship("Group", foreign_keys=[language_glottocode])


def create_tables(engine):
    Base.metadata.create_all(engine)


