from typing import List, Optional, Dict
from re import finditer


class AgentItem:
    """
    Represents a single agent (author/editor/publisher) with name and IDs.
    """
    def __init__(self, raw: str):
        self._raw = raw
        self.name: str = ""
        self.ids: List[str] = []
        self._parse(raw)
    
    def _parse(self, raw: str):
        """
        Parse the agent item string.
        Expected format: "Name, Surname [id1:xxx id2:yyy]" or just "Name, Surname" or just "[id1:xxx]"
        """
        # Extract IDs from brackets
        self.ids = [m.group() for m in finditer(r'((?:crossref|orcid|viaf|wikidata|ror|omid):\S+)(?=\s|\])', raw)]
        
        # Extract name part (everything before first '[' or the whole string if no brackets)
        bracket_pos = raw.find('[')
        if bracket_pos != -1:
            self.name = raw[:bracket_pos].strip()
        else:
            self.name = raw.strip()
    
    def to_dict(self) -> Dict:
        """
        Serialize AgentItem to dictionary.
        
        :return: Dictionary representation of the agent item
        """
        return {
            "name": self.name,
            "ids": self.ids
        }
    
    def __repr__(self):
        return f"AgentItem(name='{self.name}', ids={self.ids})"
    
    def __str__(self):
        return self._raw


class VenueInfo:
    """
    Represents venue information with name and IDs.
    """
    def __init__(self, raw: str):
        self._raw = raw
        self.name: str = ""
        self.ids: List[str] = []
        self._parse(raw)
    
    def _parse(self, raw: str):
        """
        Parse the venue string.
        Expected format: "Venue Name [id1:xxx id2:yyy]" or just "Venue Name" or just "[id1:xxx]"
        """
        # Extract IDs from brackets (using venue ID schemes)
        self.ids = [m.group() for m in finditer(r'((?:doi|issn|isbn|url|wikidata|wikipedia|openalex|omid|jid|arxiv):\S+)(?=\s|\])', raw)]
        
        # Extract name part (everything before first '[' or the whole string if no brackets)
        bracket_pos = raw.find('[')
        if bracket_pos != -1:
            self.name = raw[:bracket_pos].strip()
        else:
            self.name = raw.strip()
    
    def to_dict(self) -> Dict:
        """
        Serialize VenueInfo to dictionary.
        
        :return: Dictionary representation of the venue info
        """
        return {
            "name": self.name,
            "ids": self.ids
        }
    
    def __repr__(self):
        return f"VenueInfo(name='{self.name}', ids={self.ids})"

    def __str__(self):
        return self._raw


class MetadataRow:
    """
    Structured representation of a metadata CSV row.
    """
    def __init__(self, raw_row: Dict[str, str]):
        self._raw = raw_row.copy()
        self.id: List[str] = self._parse_id_field(raw_row.get('id', ''))
        self.title: Optional[str] = raw_row.get('title')
        self.author: Optional[List[AgentItem]] = self._parse_agent_field(raw_row.get('author'))
        self.pub_date: Optional[str] = raw_row.get('pub_date')
        self.venue: Optional[VenueInfo] = self._parse_venue_field(raw_row.get('venue'))
        self.volume: Optional[str] = raw_row.get('volume')
        self.issue: Optional[str] = raw_row.get('issue')
        self.page: Optional[str] = raw_row.get('page')
        self.type: Optional[str] = raw_row.get('type')
        self.publisher: Optional[List[AgentItem]] = self._parse_agent_field(raw_row.get('publisher'))
        self.editor: Optional[List[AgentItem]] = self._parse_agent_field(raw_row.get('editor'))
    
    def _parse_id_field(self, value: str) -> List[str]:
        """Parse ID field (space-separated list)."""
        if not value:
            return []
        return value.split(' ')
    
    def _parse_agent_field(self, value: Optional[str]) -> Optional[List[AgentItem]]:
        """Parse agent field (`; `-separated list of AgentItem)."""
        if not value:
            return None
        items = value.split('; ')
        return [AgentItem(item) for item in items]
    
    def _parse_venue_field(self, value: Optional[str]) -> Optional[VenueInfo]:
        """Parse venue field (VenueInfo)."""
        if not value:
            return None
        return VenueInfo(value)
    
    
    def flat_serialise(self) -> Dict:

        """
        Serialise MetadataRow to dictionary with ALL fields represented as lists of items (strings).
        """
        result = {
            "id": self.id,
            "title": [self.title] if self.title is not None else [],
            "author": [str(agent) for agent in self.author] if self.author is not None else [],
            "pub_date": [self.pub_date] if self.pub_date is not None else [],
            "venue": [str(self.venue)] if self.venue is not None else [],
            "volume": [ self.volume] if self.volume is not None else [],
            "issue": [self.issue] if self.issue is not None else [],
            "page": [self.page] if self.page is not None else [],
            "type": [self.type] if self.type is not None else [],
            "publisher": [str(agent) for agent in self.publisher] if self.publisher is not None else [],
            "editor": [str(agent) for agent in self.editor] if self.editor is not None else []
        }

        return result

    def __repr__(self):
        return f"MetadataRow(id={self.id}, title={self.title})"


class CitationsRow:
    """
    Structured representation of a citations CSV row.
    """
    def __init__(self, raw_row: Dict[str, str]):
        self._raw = raw_row.copy()
        self.citing_id: List[str] = self._parse_id_field(raw_row.get('citing_id', ''))
        self.citing_publication_date: Optional[str] = raw_row.get('citing_publication_date')
        self.cited_id: List[str] = self._parse_id_field(raw_row.get('cited_id', ''))
        self.cited_publication_date: Optional[str] = raw_row.get('cited_publication_date')
    
    def _parse_id_field(self, value: str) -> List[str]:
        """Parse ID field (space-separated list)."""
        if not value:
            return []
        return value.split(' ')
    
    def flat_serialise(self) -> Dict:
        """
        Serialise CitationsRow to dictionary with ALL fields represented as lists of items (strings).
        """
        result = {
            "citing_id": self.citing_id,
            "citing_publication_date": [self.citing_publication_date] if self.citing_publication_date is not None else [],
            "cited_id": self.cited_id,
            "cited_publication_date": [self.cited_publication_date] if self.cited_publication_date is not None else []
        }

        return result
    
    def __repr__(self):
        return f"CitationsRow(citing_id={self.citing_id}, cited_id={self.cited_id})"


def read_metadata_row(row_dict: Dict[str, str]) -> MetadataRow:
    """
    Read and parse a metadata CSV row into a structured MetadataRow object.
    
    :param row_dict: Dictionary representing a single CSV row (from csv.DictReader)
    :return: MetadataRow object with parsed fields
    """
    return MetadataRow(row_dict)


def read_citations_row(row_dict: Dict[str, str]) -> CitationsRow:
    """
    Read and parse a citations CSV row into a structured CitationsRow object.
    
    :param row_dict: Dictionary representing a single CSV row (from csv.DictReader)
    :return: CitationsRow object with parsed fields
    """
    return CitationsRow(row_dict)