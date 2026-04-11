from typing import List, Optional, Dict
from re import finditer


class AgentItem:
    """
    Represents a single agent (author, editor, or publisher) with a name and
    zero or more responsible-agent identifiers.
    """
    def __init__(self, raw: str) -> None:
        """
        Parse a raw agent string into name and IDs.

        :param raw: The raw agent string (e.g. ``"Smith, John [orcid:0000-0001]"``).
        :type raw: str
        :rtype: None
        """
        self._raw = raw
        self.name: str = ""
        self.ids: List[str] = []
        self._parse(raw)

    def _parse(self, raw: str) -> None:
        """
        Extract name and bracketed IDs from the raw string.

        :param raw: The raw agent string.
        :type raw: str
        :rtype: None
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
        Serialize the agent item to a dictionary.

        :return: Dictionary with ``"name"`` and ``"ids"`` keys.
        :rtype: Dict
        """
        return {
            "name": self.name,
            "ids": self.ids
        }

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the agent item."""
        return f"AgentItem(name='{self.name}', ids={self.ids})"

    def __str__(self) -> str:
        """Return the original raw string."""
        return self._raw


class VenueInfo:
    """
    Represents venue information with a name and zero or more bibliographic-resource
    identifiers.
    """
    def __init__(self, raw: str) -> None:
        """
        Parse a raw venue string into name and IDs.

        :param raw: The raw venue string (e.g. ``"Nature [issn:1234-5678]"``).
        :type raw: str
        :rtype: None
        """
        self._raw = raw
        self.name: str = ""
        self.ids: List[str] = []
        self._parse(raw)

    def _parse(self, raw: str) -> None:
        """
        Extract name and bracketed IDs from the raw venue string.

        :param raw: The raw venue string.
        :type raw: str
        :rtype: None
        """
        # Extract IDs from brackets (using venue ID schemes)
        self.ids = [m.group() for m in finditer(r'((?:doi|issn|isbn|url|wikidata|wikipedia|openalex|omid|jid|arxiv|pmid):\S+)(?=\s|\])', raw)]

        # Extract name part (everything before first '[' or the whole string if no brackets)
        bracket_pos = raw.find('[')
        if bracket_pos != -1:
            self.name = raw[:bracket_pos].strip()
        else:
            self.name = raw.strip()

    def to_dict(self) -> Dict:
        """
        Serialize the venue info to a dictionary.

        :return: Dictionary with ``"name"`` and ``"ids"`` keys.
        :rtype: Dict
        """
        return {
            "name": self.name,
            "ids": self.ids
        }

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the venue info."""
        return f"VenueInfo(name='{self.name}', ids={self.ids})"

    def __str__(self) -> str:
        """Return the original raw string."""
        return self._raw


class MetadataRow:
    """
    Structured representation of a metadata (META-CSV) row.

    Each field is parsed into its appropriate type (lists of strings for IDs,
    :class:`AgentItem` lists for author/editor/publisher, etc.).
    """
    def __init__(self, raw_row: Dict[str, str]) -> None:
        """
        Parse a raw CSV row dictionary into a structured MetadataRow.

        :param raw_row: Dictionary mapping column names to raw string values.
        :type raw_row: Dict[str, str]
        :rtype: None
        """
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
        """
        Parse a space-separated ID field into a list of strings.

        :param value: Raw space-separated ID string.
        :type value: str
        :return: List of individual ID strings, or an empty list if blank.
        :rtype: List[str]
        """
        if not value:
            return []
        return value.split(' ')

    def _parse_agent_field(self, value: Optional[str]) -> Optional[List[AgentItem]]:
        """
        Parse a semicolon-separated agent field into a list of AgentItem objects.

        :param value: Raw agent field string, or ``None`` if empty.
        :type value: Optional[str]
        :return: List of :class:`AgentItem` instances, or ``None`` if blank.
        :rtype: Optional[List[AgentItem]]
        """
        if not value:
            return None
        items = value.split('; ')
        return [AgentItem(item) for item in items]

    def _parse_venue_field(self, value: Optional[str]) -> Optional[VenueInfo]:
        """
        Parse the venue field into a VenueInfo object.

        :param value: Raw venue string, or ``None`` if empty.
        :type value: Optional[str]
        :return: :class:`VenueInfo` instance, or ``None`` if blank.
        :rtype: Optional[VenueInfo]
        """
        if not value:
            return None
        return VenueInfo(value)


    def flat_serialise(self) -> Dict:
        """
        Serialise the row to a flat dictionary where every field value is a list of strings.

        Multi-item fields (IDs, agents) are represented as lists of their raw
        string forms; single-value fields are wrapped in a one-element list.

        :return: Dictionary mapping field names to lists of string items.
        :rtype: Dict
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

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the metadata row."""
        return f"MetadataRow(id={self.id}, title={self.title})"


class CitationsRow:
    """
    Structured representation of a citations (CITS-CSV) row.

    Parses citing and cited ID fields and optional publication dates.
    """
    def __init__(self, raw_row: Dict[str, str]) -> None:
        """
        Parse a raw CSV row dictionary into a structured CitationsRow.

        :param raw_row: Dictionary mapping column names to raw string values.
        :type raw_row: Dict[str, str]
        :rtype: None
        """
        self._raw = raw_row.copy()
        self.citing_id: List[str] = self._parse_id_field(raw_row.get('citing_id', ''))
        self.citing_publication_date: Optional[str] = raw_row.get('citing_publication_date')
        self.cited_id: List[str] = self._parse_id_field(raw_row.get('cited_id', ''))
        self.cited_publication_date: Optional[str] = raw_row.get('cited_publication_date')

    def _parse_id_field(self, value: str) -> List[str]:
        """
        Parse a space-separated ID field into a list of strings.

        :param value: Raw space-separated ID string.
        :type value: str
        :return: List of individual ID strings, or an empty list if blank.
        :rtype: List[str]
        """
        if not value:
            return []
        return value.split(' ')

    def flat_serialise(self) -> Dict:
        """
        Serialise the row to a flat dictionary where every field value is a list of strings.

        :return: Dictionary mapping field names to lists of string items.
        :rtype: Dict
        """
        result = {
            "citing_id": self.citing_id,
            "citing_publication_date": [self.citing_publication_date] if self.citing_publication_date is not None else [],
            "cited_id": self.cited_id,
            "cited_publication_date": [self.cited_publication_date] if self.cited_publication_date is not None else []
        }

        return result

    def __repr__(self) -> str:
        """Return an unambiguous string representation of the citations row."""
        return f"CitationsRow(citing_id={self.citing_id}, cited_id={self.cited_id})"


def read_metadata_row(row_dict: Dict[str, str]) -> MetadataRow:
    """
    Parse a metadata CSV row into a structured :class:`MetadataRow` object.

    :param row_dict: Dictionary representing a single CSV row (from ``csv.DictReader``).
    :type row_dict: Dict[str, str]
    :return: Parsed :class:`MetadataRow` instance.
    :rtype: MetadataRow
    """
    return MetadataRow(row_dict)


def read_citations_row(row_dict: Dict[str, str]) -> CitationsRow:
    """
    Parse a citations CSV row into a structured :class:`CitationsRow` object.

    :param row_dict: Dictionary representing a single CSV row (from ``csv.DictReader``).
    :type row_dict: Dict[str, str]
    :return: Parsed :class:`CitationsRow` instance.
    :rtype: CitationsRow
    """
    return CitationsRow(row_dict)
