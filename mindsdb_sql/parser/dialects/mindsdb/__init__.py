from .create_view import CreateView
from .create_database import CreateDatabase
from .create_predictor import CreatePredictor
from .drop_predictor import DropPredictor
from .retrain_predictor import RetrainPredictor
from .finetune_predictor import FinetunePredictor
from .drop_integration import DropIntegration
from .drop_datasource import DropDatasource
from .drop_dataset import DropDataset
from .evaluate import Evaluate
from .latest import Latest
from .create_file import CreateFile
from .create_ml_engine import CreateMLEngine
from .drop_ml_engine import DropMLEngine
from .create_job import CreateJob
from .drop_job import DropJob
from .chatbot import CreateChatBot, DropChatBot

# remove it in next release
CreateDatasource = CreateDatabase
