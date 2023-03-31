from typing import List, Dict, Optional, Callable
from dataclasses import dataclass
from urllib.parse import urljoin
import logging

from requests_toolbelt.sessions import BaseUrlSession

logger = logging.getLogger(__name__)

@dataclass
class PhabricatorObject:
    id: str
    phid: str
    author_phid: str
    timestamp: int
    contents: str

@dataclass
class PhabricatorWiki(PhabricatorObject):
    name: str

@dataclass
class PhabricatorTask(PhabricatorObject):
    name: str
    
@dataclass
class PhabricatorComment(PhabricatorObject):
    pass

@dataclass
class PhabricatorClient:
    url: str
    token: str
    verify_ssl: Optional[bool] = True
    
    def __post_init__(self):
        api_url = urljoin(self.url, "/api/")
        self.session = BaseUrlSession(base_url=api_url)
    
    
    def check_server_health(self):
        BaseUrlSession(base_url=self.url).get("/", verify=self.verify_ssl).raise_for_status()
    
    
    def validate_authentication(self):
        self._get_with_auth("/").raise_for_status()
    
    
    def _get_with_auth(self, url, *args, **kwargs):
        params_with_token = {"api.token": self.token}
        if "params" in kwargs:
            params_with_token.update(kwargs["params"])
        
        response = self.session.get(url, verify=self.verify_ssl, params=params_with_token, *args, **kwargs)
        response.raise_for_status()
        return response
    
    
    def _query_endpoint(self, endpoint, query: Dict) -> Dict:
        response = self._get_with_auth(endpoint, data=query)
        return response.json()["result"]
    
    def _query_maniphest(self, query: Dict) -> Dict:
        endpoint = "maniphest.search"
        return self._query_endpoint(endpoint, query)
    
    def _query_transaction(self, query: Dict) -> Dict:
        endpoint = "transaction.search"
        return self._query_endpoint(endpoint, query)
    
    def _query_phriction(self, query: Dict) -> Dict:
        endpoint = "phriction.content.search"
        return self._query_endpoint(endpoint, query)
    
    
    @staticmethod
    def _get_paging_direction(cursor: Dict):
        if cursor["before"] is not None:
            return "before"
        elif cursor["after"] is not None:
            return "after"
        return
   
    
    def _get_objects(self, endpoint: Callable, parser: Callable, query: Dict, *args, **kwargs):
        paging_direction: Optional[str] = None
        is_first_page = True
        
        parsed_objects = []
        while True:
            results = endpoint(query)
            
            if results is None:
                logging.info(f"Recieved empty response from {self.url} - endpoint: {str(endpoint)}")
                break
            
            parsed_objects.extend(parser(results, *args, **kwargs))
        
            next_page_cursor = results["cursor"]
                
            if is_first_page:
                paging_direction = self._get_paging_direction(next_page_cursor)
                is_first_page = False

            if paging_direction is None or next_page_cursor[paging_direction] is None:
                break
                
            query[paging_direction] = next_page_cursor[paging_direction]
        return parsed_objects
            

    @staticmethod
    def _comment_parser(results: Dict, date_modified: int):
        parsed_comments = []
        
        for transaction in results["data"]:
            if transaction["type"] == "comment":
                comment = transaction["comments"][0]
                
                if comment["dateModified"] >= date_modified:
                    parsed_comments.append(
                        PhabricatorComment(id=comment["id"],
                                           phid=comment["phid"],
                                           contents=comment["content"]["raw"],
                                           timestamp=comment["dateModified"],
                                           author_phid=comment["authorPHID"])
                    )
        return parsed_comments
    
    
    @staticmethod
    def _task_parser(results: Dict):
        return  [
            PhabricatorTask(id=task["id"],
                            name=task["fields"]["name"],
                            contents=task["fields"]["description"]["raw"],
                            timestamp=task["fields"]["dateModified"],
                            phid=task["phid"],
                            author_phid=task["fields"]["authorPHID"]) 
            for task in results["data"]
        ]
    
    
    @staticmethod
    def _wiki_parser(results: Dict, date_modified: int) -> List[PhabricatorWiki]:
        parsed_wikis = {}
        for wiki in results["data"]:
            if wiki["fields"]["dateModified"] >= date_modified:
                phid = wiki["phid"]
                parsed_wikis[phid] = PhabricatorWiki(
                    id=wiki["id"],
                    phid=phid,
                    author_phid=wiki["attachments"]["content"]["authorPHID"],
                    timestamp=wiki["fields"]["dateModified"],
                    name=wiki["attachments"]["content"]["title"],
                    contents=wiki["attachments"]["content"]["content"]["raw"]
                )
        return list(parsed_wikis.values())
        

    def get_object_comments(self, obj: PhabricatorObject, date_modified: int):
        query = {"objectIdentifier": obj.phid}
        return self._get_objects(
            self._query_transaction,
            self._comment_parser,
            query,
            date_modified=date_modified
        )
    
    
    def get_all_tasks(self, date_modified: int) -> List[PhabricatorTask]:
        query = {"queryKey": "all", "constraints[modifiedStart]": date_modified}
        return self._get_objects(
            self._query_maniphest,
            self._task_parser,
            query
        )

    
    def get_all_wikis(self, date_modified: int):
        query = {
            "queryKey": "all",
            "attachments[content]": True,
            "order": "newest"
        }
        return self._get_objects(
            self._query_phriction,
            self._wiki_parser,
            query,
            date_modified=date_modified
        )
