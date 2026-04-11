# Copyright (c) 2023, OpenCitations <contact@opencitations.net>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from oc_ds_converter.oc_idmanager import doi, isbn, issn, orcid, pmcid, pmid, ror, url, viaf, wikidata, wikipedia, \
    openalex, crossref, jid, arxiv
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import logging

logger = logging.getLogger('oc_validator')


class IdExistence:
    """
    Checks whether an external identifier actually exists by querying either
    the appropriate external service API, the OpenCitations Meta triplestore,
    or both.
    """

    def __init__(self, use_meta_endpoint: bool = True) -> None:
        """
        Initialise ID managers and the SPARQL endpoint for existence checks.

        :param use_meta_endpoint: Whether to query the OC Meta triplestore
            before falling back to external services. Defaults to ``True``.
        :type use_meta_endpoint: bool
        :rtype: None
        """
        self.doi_mngr = doi.DOIManager()
        self.isbn_mngr = isbn.ISBNManager()
        self.issn_mngr = issn.ISSNManager()
        self.orcid_mngr = orcid.ORCIDManager()
        self.pmcid_mngr = pmcid.PMCIDManager()
        self.pmid_mngr = pmid.PMIDManager()
        self.ror_mngr = ror.RORManager()
        self.url_mngr = url.URLManager()
        self.viaf_mngr = viaf.ViafManager()
        self.wikidata_mngr = wikidata.WikidataManager()
        self.wikipedia_mngr = wikipedia.WikipediaManager()
        self.openalex_mngr = openalex.OpenAlexManager()
        self.crossref_mngr = crossref.CrossrefManager()
        self.jid_mngr = jid.JIDManager()
        self.arxiv_mngr = arxiv.ArXivManager()
        self.use_meta_endpoint = use_meta_endpoint
        self.sparql = SPARQLWrapper("https://sparql.opencitations.net/meta")
        self.sparql.addCustomHttpHeader('User-Agent', 'oc_validator')

    def check_id_existence(self, id: str) -> bool:
        """
        Check whether an identifier exists in external services or Meta.

        If ``use_meta_endpoint`` is ``False``, only external services are queried.
        If ``True``, the OC Meta triplestore is queried first; if the ID is found
        there, ``True`` is returned immediately. Otherwise, the external service
        is queried as a fallback.

        :param id: The identifier string, including its prefix.
        :type id: str
        :return: ``True`` if the identifier is confirmed to exist, ``False`` otherwise.
        :rtype: bool
        """
        if id.startswith('temp:') or id.startswith('local:'): # temp: and local: internal IDs are always considered as exisiting
            return True
        if id.startswith('omid:'):  # OMID needs to be checked with a specific query on the triplestore
            return self.query_omid_in_meta(id)
        if self.use_meta_endpoint:
            meta_response = self.query_meta_triplestore(id)
            return meta_response if meta_response is True else self.query_external_service(id)
        return self.query_external_service(id)

    def query_external_service(self, id: str) -> bool:
        """
        Check whether an identifier is registered in its native service.

        Dispatches to the appropriate manager's ``exists()`` method based on
        the identifier prefix.

        :param id: The identifier string, including its prefix.
        :type id: str
        :return: ``True`` if the identifier exists in the external service, ``False`` otherwise.
        :rtype: bool
        """
        oc_prefix = id[:(id.index(':') + 1)]

        if oc_prefix == 'doi:':
            vldt = self.doi_mngr
        elif oc_prefix == 'isbn:':
            vldt = self.isbn_mngr
        elif oc_prefix == 'issn:':
            vldt = self.issn_mngr
        elif oc_prefix == 'orcid:':
            vldt = self.orcid_mngr
        elif oc_prefix == 'pmcid:':
            vldt = self.pmcid_mngr
        elif oc_prefix == 'pmid:':
            vldt = self.pmid_mngr
        elif oc_prefix == 'ror:':
            vldt = self.ror_mngr
        elif oc_prefix == 'url:':
            vldt = self.url_mngr
        elif oc_prefix == 'viaf:':
            vldt = self.viaf_mngr
        elif oc_prefix == 'wikidata:':
            vldt = self.wikidata_mngr
        elif oc_prefix == 'wikipedia:':
            vldt = self.wikipedia_mngr
        elif oc_prefix == 'openalex:':
            vldt = self.openalex_mngr
        elif oc_prefix == 'crossref:':
            vldt = self.crossref_mngr
        elif oc_prefix == 'jid:':
            vldt = self.jid_mngr
        elif oc_prefix == 'arxiv:':
            vldt = self.arxiv_mngr
        else:
            return False
        return vldt.exists(id.replace(oc_prefix, '', 1))

    def query_meta_triplestore(self, id: str, retries: int = 3, delay: float = 2.0) -> bool:
        """
        Check whether an identifier exists in the OpenCitations Meta triplestore via SPARQL.

        Retries the query up to *retries* times with a *delay* (in seconds)
        between attempts on transient failures.

        :param id: The identifier string, including its prefix.
        :type id: str
        :param retries: Maximum number of query attempts. Defaults to 3.
        :type retries: int
        :param delay: Seconds to wait between retries. Defaults to 2.0.
        :type delay: float
        :return: ``True`` if the triplestore confirms the ID exists, ``False`` otherwise.
        :rtype: bool
        """
        oc_prefix = id[:(id.index(':') + 1)]
        lookup_id = id.replace(oc_prefix, '', 1)
        datacite_id_scheme = oc_prefix[:-1]  # same as OC prefix but without the ":"

        sparql = self.sparql
        q = '''
        PREFIX datacite: <http://purl.org/spar/datacite/>
        PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        ASK {
            VALUES ?val { "%s" "%s"^^xsd:string }
            ?identifier literal:hasLiteralValue ?val .
            ?res datacite:hasIdentifier ?identifier .
            ?identifier datacite:usesIdentifierScheme datacite:%s .
        }
        ''' % (lookup_id, lookup_id, datacite_id_scheme)

        for attempt in range(retries):
            try:
                sparql.setQuery(q)
                sparql.setReturnFormat(JSON)
                result: dict = sparql.query().convert()
                return result.get('boolean')
            
            except Exception as e:
                logger.warning("SPARQL query attempt %d/%d failed for '%s': %s", attempt + 1, retries, id, e)
                if attempt < retries - 1:
                    time.sleep(delay)  # wait before retrying
                else:
                    logger.warning("Max retries reached for SPARQL query on '%s'. Query failed.", id)
                    return False

    def query_omid_in_meta(self, id: str, retries: int = 3, delay: float = 2.0) -> bool:
        """
        Check whether an OMID is registered in the OpenCitations Meta triplestore.

        This uses a dedicated SPARQL query that checks for the OMID as both
        subject and object. Retries on transient failures.

        :param id: The OMID string, including the ``omid:`` prefix.
        :type id: str
        :param retries: Maximum number of query attempts. Defaults to 3.
        :type retries: int
        :param delay: Seconds to wait between retries. Defaults to 2.0.
        :type delay: float
        :return: ``True`` if the OMID exists in Meta, ``False`` otherwise.
        :rtype: bool
        """
        lookup_id = id.replace('omid:', '', 1)

        sparql = self.sparql

        q = '''
        ASK WHERE {
            { <https://w3id.org/oc/meta/%s> ?p ?o } 
        UNION 
            { ?s ?p <https://w3id.org/oc/meta/%s> }
        }
        ''' % (lookup_id, lookup_id)

        for attempt in range(retries):
            try:
                sparql.setQuery(q)
                sparql.setReturnFormat(JSON)
                result: dict = sparql.query().convert()
                return result.get('boolean', False)
            
            except Exception as e:
                logger.warning("OMID SPARQL query attempt %d/%d failed for '%s': %s", attempt + 1, retries, id, e)
                if attempt < retries - 1:
                    time.sleep(delay)  # wait before retrying
                else:
                    logger.warning("Max retries reached for OMID SPARQL query on '%s'. Query failed.", id)
                    return False