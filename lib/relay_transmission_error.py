
import re
import path
from pathlib import *
import os

class RelayTransmissionError(Exception):
    """
    Module specific exception for RelaySftp class.
    """
    pass


def incremental_file_name(original_fname):
    m = re.search("-(\d+)\.(.*)", original_fname)
    
    new_name = None
    bfn = os.path.basename(original_fname)
    
    if m:  # if has ending like "-999.dat"
        oldnum = m.group(1)
        newnum = int(oldnum) + 1
        new_name = re.sub("-(\d+)\.(.*)", "-"+str(newnum)+"." + m.group(2), bfn)
        
    else:  # no index, just appending "-1"
        p = PurePosixPath(original_fname)
        new_name = p.stem + "-1" + p.suffix
    
    if  new_name:
        return os.path.join(os.path.dirname(original_fname), new_name )
    else:
        return orignal_fname
                    