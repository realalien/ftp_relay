"""
Rewrite of ftp2.py to provide similar API for file transfer, wrapping the pysftp module.
NOTE: Please remember to install pysftp first, ```pip install pysftp```.

__version__ = "1.0.0"
__author__ = "Alien Zhu <jiacheng.zhu@pdf.com>"
"""

import logging
import os
import pysftp
from .relay_transmission_error import *


class RelaySftp:

    def __init__(self, host, login, passwd, dir):
        """
        Class initializer.

        :param dryrun: boolean to enable non-destructive log only mode
        :param fdir: path to file forward directory
        :param qdir: path to file quarantine directory
        """
        self.log = logging.getLogger(__name__)
        self.ftp_conn = None  # handle for open FTP connection to YMS host

        self.ftp_host = host
        self.ftp_login = login
        self.ftp_passwd = passwd
        self.ftp_dir = dir
        return

    def ftp_open(self):
        """
        Opens connection to FTP host.

        :return: None
        """
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None

            self.ftp_conn = pysftp.Connection(self.ftp_host, username=self.ftp_login, password=self.ftp_passwd,
                                              cnopts=cnopts)

            self.log.info("Connected to SFTP host {0}".format(self.ftp_host))
            # Note: do not set timeout before any ftp action, the _sftp, created from paramiko.SFTPClient.from_transport(), wouldn't be initlized until a real ftp action

            self.ftp_conn.chdir(self.ftp_dir)
            self.ftp_conn._sftp.get_channel().settimeout(60) #Credit:     https://goo.gl/9RNF7v

            self.log.info("Changed to SFTP server directory {0}".format(self.ftp_dir))
        except Exception as ex:
            self.log.exception(ex)
            raise RelayTransmissionError("Exception opening FTP connection to host {0}".format(self.ftp_host)) from ex
        return
        
    def ftp_upload(self, file):
        """
        Forwards file via FTP to YMS host for loading.

        Note: Reference how SSH treats all files as binary, REF: https://stackoverflow.com/questions/14646185/ftplib-retrbinary-in-paramiko

        :param file: file being processed
        :return: True for success, otherwise False   """
        result = False
        new_name = None
        try:
            bfn = os.path.basename(file)  # base file name stripped of leading path
            tmp = bfn + ".tmp"
            
            new_name = None
            # REF: see doc, https://goo.gl/kC9Xjo
            localpath = os.path.dirname(file)
            
            # remove potential corrupted previous upload
            if self.ftp_conn.exists(tmp):
                self.ftp_conn.unlink(tmp)
                self.log.warn("Found existing .tmp {0}, removed it.".format(tmp))
            
            # rename original file by incremental number, e.g. a.dat, a-1.dat, a-2.dat
            true_file = bfn
            while self.ftp_conn.exists(true_file): # to get real unused name
                true_file = incremental_file_name(true_file)    
        
            if true_file != bfn:  # updated
                self.log.info("Temporarily renaming {0} to {1}".format(bfn, true_file) )
                new_name = os.path.join(localpath, true_file)
                os.rename(file, new_name)    
            
            if new_name:
                bfn = true_file
                tmp =  bfn + ".tmp"
                            
            #self.ftp_conn._sftp.get_channel().settimeout(60) #time is in seconds, Credit:     https://goo.gl/9RNF7v
            self.ftp_conn.put(os.path.join(localpath, bfn), tmp)
            self.ftp_conn.rename(tmp, bfn)  # rename to canonical file name

            result = True
            # Notes: remove_file actioin occurs in the caller to make code module-like and less coupling
        except Exception as ex: 
            self.log.exception(ex)
            raise RelayTransmissionError("Exception uploading file {0}".format(file)) from ex
        finally:
            # always to rename back for other to upload
            if new_name:
                self.log.info("Renaming back from {0} to {1}".format(new_name, file) )
                os.rename(new_name, file) 
                
            return result
        return False

    def ftp_close(self):
        """
        Closes connection to FTP host.

        :return: None
        """
        try:
            if self.ftp_conn:
                self.ftp_conn.close()
        except Exception as ex:
            self.log.exception(ex)
            raise "Exception closing FTP connection to host {0}".format(self.ftp_host) from ex
        return


if __name__ == '__main__':
    # demo
    if not os.path.exists("./log"):
        os.makedirs("./log")
    logfile = "./log/test_sftp.log"
    LOG_FMT = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=LOG_FMT)
    log = logging.getLogger(__name__)
    f = RelaySftp("10.10.90.171", "dpower", "eqpxfs!", "project/cassandra/data/ftp")
    f.ftp_open()

    file2transfer = "./output_from_sftp.csv"
    absp = os.path.abspath(file2transfer)
    log.info(absp)
    if os.path.exists(absp):
        f.ftp_upload(file2transfer)
    else:
        log.info("File does not exist!")
    f.ftp_close()
    log.info("Done transfering, please check manually!")
    logging.shutdown()
