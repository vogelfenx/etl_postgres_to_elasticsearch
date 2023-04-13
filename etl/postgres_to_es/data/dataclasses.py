from dataclasses import dataclass, field
from typing import List
from uuid import UUID


@dataclass
class Movie:
    id: UUID
    imdb_rating: float
    title: str
    description: str
    director: str = None
    genres: List[str] = field(default_factory=list)
    actors_names: List[str] = field(default_factory=list)
    writers_names: List[str] = field(default_factory=list)
    actors: dict[int, str] = field(default_factory=dict)
    writers: dict[int, str] = field(default_factory=dict)
