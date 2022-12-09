from .create_view import CreateView
from .create_database import CreateDatabase
from .create_predictor import CreatePredictor
from .drop_predictor import DropPredictor
from .retrain_predictor import RetrainPredictor
from .adjust_predictor import AdjustPredictor
from .drop_integration import DropIntegration
from .drop_datasource import DropDatasource
from .drop_dataset import DropDataset
from .latest import Latest
from .create_file import CreateFile
from .create_ml_engine import CreateMLEngine
from .drop_ml_engine import DropMLEngine

# Temporary
CreateDatasource = CreateDatabase
