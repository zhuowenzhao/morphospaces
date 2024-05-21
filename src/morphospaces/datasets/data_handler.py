import s3fs 
import glob
import mrcfile
from mrcfile.mrcinterpreter import MrcInterpreter

from typing import Dict, List, Tuple, Union
from numpy.typing import ArrayLike


class MrcFile:
    """
    Handle loading/writing data from local disk or s3 bucket.    
    """
    def __init__(self):
        pass
    
    @staticmethod
    def read(file_path):
        if file_path.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            with s3.open(file_path, 'rb') as f:
                ds = MrcInterpreter(iostream=f, permissive=True).data.copy()
        else:
            ds = mrcfile.read(file_path)
        return ds
    
    @staticmethod
    def glob(glob_pattern):
        if glob_pattern.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            return ['s3://' + fp for fp in s3.glob(glob_pattern)]
        else:
            return glob.glob(glob_pattern)