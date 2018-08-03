from ftplib import FTP
import os
import logging
from subprocess import call
from .relay_transmission_error import *


class RelayFtp(object):
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
            self.log.info("Connecting to : {0}".format(self.ftp_host))
            self.ftp_conn = FTP(self.ftp_host, timeout=60)
            self.ftp_conn.login(self.ftp_login, self.ftp_passwd)
            self.log.info("Connected to FTP host {0}".format(self.ftp_host))
            self.ftp_conn.set_pasv(True)
            self.log.info("Using PASSIVE FTP mode.")
            self.ftp_conn.cwd(self.ftp_dir)
            self.log.info("Changed to FTP server directory {0}".format(self.ftp_dir))
        except Exception as ex:
            self.log.exception(ex)
            raise RelayTransmissionError("Exception opening FTP connection to host {0}".format(self.ftp_host)) from ex
        return

    def ftp_upload(self, file):
        """
        Forwards file via FTP to YMS host for loading.

        :param file: file being processed
        :return: True if success, otherwise False
        """
        result = False
        new_name = None
        try:
            bfn = os.path.basename(file)  # base file name stripped of leading path
            tmp = bfn + ".tmp"
            localpath = os.path.dirname(file)
            realfile = file
            
            # NOTE: it may cause performance if server has much files or bandwidth is not good
            # remove potential corrupted previous upload
            files_on_server = self.ftp_conn.nlst()
            if tmp in files_on_server :
                self.ftp_conn.delete(tmp)
                self.log.warn("Found existing .tmp {0}, removed it.".format(tmp))
            

            # rename original file by incremental number, e.g. a.dat, a-1.dat, a-2.dat
            true_file = bfn
            
            while true_file in files_on_server: # to get real unused name
                true_file = incremental_file_name(true_file)
                files_on_server = self.ftp_conn.nlst()
        
            if true_file != bfn:  # updated
                self.log.info("Temporarily renaming {0} to {1}".format(bfn, true_file) )
                new_name = os.path.join(localpath, true_file)
                os.rename(file, new_name)    
            
            if new_name:
                bfn = true_file
                tmp =  bfn + ".tmp"
                realfile = os.path.join( os.path.dirname(file) , bfn)
            
            cmd = "STOR {0}".format(tmp)
            self.ftp_conn.storbinary(cmd, open(realfile, mode='rb'), 8192)  # upload named with tmp extension
            self.ftp_conn.rename(tmp, bfn)  # rename to canonical file name
            result = True
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
                self.ftp_conn.quit()
        except Exception as ex:
            self.log.exception(ex)
            raise RelayTransmissionError("Exception closing FTP connection to host {0}".format(self.ftp_host)) from ex
        return


if __name__ == '__main__':
    # create example file if not exists
    call(["touch", "output.csv"])

    if not os.path.exists("./log"):
        os.makedirs("./log")
    logfile = "./log/test_ftp.log"
    LOG_FMT = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=LOG_FMT)
    log = logging.getLogger(__name__)
    # while True:
    f = RelayFtp(log, "10.10.90.171", "dpower", "eqpxfs!", "project/cassandra/data/ftp")
    f.ftp_open()
    f.ftp_upload('./output.csv')
    f.ftp_close()
    logging.shutdown()
