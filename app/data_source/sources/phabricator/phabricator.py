import logging
from datetime import datetime
from typing import List, Dict

from data_source.api.base_data_source import BaseDataSource, ConfigField, HTMLInputType
from data_source.api.basic_document import BasicDocument, DocumentType
from data_source.api.exception import InvalidDataSourceConfig
from data_source.api.utils import parse_with_workers
from queues.index_queue import IndexQueue
from data_source.sources.phabricator.client import PhabricatorClient, PhabricatorComment, PhabricatorObject

logger = logging.getLogger(__name__)

class PhabricatorDataSource(BaseDataSource):
    FEED_BATCH_SIZE = 512
    
    @staticmethod
    def get_config_fields() -> List[ConfigField]:
        return [
            ConfigField(label="Phabricator Server", name="url", placeholder="https://phabricator.server.com/",
                        input_type=HTMLInputType.TEXT),
            ConfigField(label="Access Token", name="token", placeholder="paste-your-access-token-here",
                        input_type=HTMLInputType.PASSWORD),
        ]


    @staticmethod
    def validate_config(config: Dict) -> None:
        try:
            # @@@ CRITICAL !!!! REMOVE VERIFY_SSL = FALSE 
            phab = PhabricatorClient(**config, verify_ssl=False)
            phab.check_server_health()
            phab.validate_authentication()
        except Exception as e:
            raise InvalidDataSourceConfig from e


    @property
    def _last_index_time_timestamp(self):
        return int(self._last_index_time.timestamp())


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # @@@ CRITICAL !!!! REMOVE VERIFY_SSL = FALSE 
        self._phab = PhabricatorClient(**self._config, verify_ssl=False)


    def _feed_new_documents(self) -> None:
        phab_objects = self._fetch_all_objects()
        phab_objects.extend(self._fetch_all_comments(phab_objects))
        self._index_all_documents(phab_objects)

    
    def _index_all_documents(self, documents: List[PhabricatorObject]):
        parsed_documents = []
        total_fed = 0
        for document in documents:
            if isinstance(document, PhabricatorComment):
                title = "Comment"
            else:
                title = document.name
            
            parsed_documents.append(
                BasicDocument(
                    id=document.id,
                    data_source_id=self._data_source_id,
                    type=DocumentType.COMMENT,
                    title=title,
                    content=document.contents,
                    timestamp=datetime.fromtimestamp(document.timestamp),
                    author=document.author_phid,
                    author_image_url="@@@",
                    location="@@@",
                    url="@@@",
                )
            )
            if len(parsed_documents) >= PhabricatorDataSource.FEED_BATCH_SIZE:
                total_fed += len(parsed_documents)
                IndexQueue.get().feed(docs=parsed_documents)
                parsed_documents = []

        total_fed += len(parsed_documents)
        IndexQueue.get().feed(docs=parsed_documents)
        if total_fed > 0:
            logging.info(f'Worker fed {total_fed} documents')
                
    
    
    def _fetch_all_objects(self):
        wikis = self._phab.get_all_wikis(self._last_index_time_timestamp)
        tasks = self._phab.get_all_tasks(self._last_index_time_timestamp)
        
        all_documents = []
        all_documents.extend(tasks)
        all_documents.extend(wikis)
        
        return all_documents
    
    
    def _fetch_all_comments(self, documents: List[PhabricatorObject]):
        comments = []
        for document in documents:
            comments.extend(self._phab.get_object_comments(document, self._last_index_time_timestamp))
        return comments