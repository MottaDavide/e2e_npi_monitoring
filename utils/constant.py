import platform
import os
from pathlib import Path

# Discovery OS
OS = platform.system() # Darwin = Mac
USER = os.getlogin()
MAC_OS = 'Darwin'
WINDOWS_OS = "Windows"
PATH_DATA = Path(__file__).parents[2] / "data"