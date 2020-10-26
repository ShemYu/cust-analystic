import os
import sys

_INIT_FOLDERS = ['bin', 'doc', 'tests']


if __name__ == "__main__":
    [os.mkdir(folder_name) for folder_name in _INIT_FOLDERS if not os.path.exists(folder_name)]
            