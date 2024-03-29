from dataclasses import dataclass, field
from typing import List
from uuid import UUID


@dataclass
class Movie:
    id: UUID
    imdb_rating: float
    title: str
    description: str
    director: str = ''
    genre: List[str] = field(default_factory=list)
    actors_names: List[str] = field(default_factory=list)
    writers_names: List[str] = field(default_factory=list)
    actors: List[dict] = field(default_factory=list)
    writers: List[dict] = field(default_factory=list)
