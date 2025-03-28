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
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_language = True


def create_tables(engine):
    Base.metadata.create_all(engine)

