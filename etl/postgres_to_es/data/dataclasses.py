from dataclasses import dataclass, field
from typing import List, Set
from uuid import UUID


@dataclass
class Movie:
    id: UUID
    imdb_rating: float
    title: str
    description: str
    director: str = None
    genres: Set[str] = field(default_factory=set)
    actors_names: Set[str] = field(default_factory=set)
    writers_names: Set[str] = field(default_factory=set)
    actors: List[dict] = field(default_factory=list)
    writers: List[dict] = field(default_factory=list)
