"""
Utility to forward copies of key data files from TDS to YMS for loading.

Invoke with a command of the form:
     D:\Python35\python.exe tds_relay.py  -r D:\DataBaseDirectory
"""
"""
COPYRIGHT
This utility is Copyright PDF Solutions, Inc. 2017 all rights reserved.

LICENSE AGREEMENT
HiSilicon deployment and use of this utility is subject to the terms and
conditions of the SOFTWARE LICENSE & RELATED SERVICES AGREEMENT between
PDF Solutions, Inc. and Huawei Technologies Co. Ltd.  This utility is
Software, as defined in the foregoing Software License Agreement.
"""
__version__ = "0.9.1"
__author__ = "Alien Zhu <jiacheng.zhu@pdf.com>"
__revision__ = "12fbcce"


import argparse
from datetime import datetime
import glob
import logging
import os
import sys
import shutil
import time
import configparser
import zipfile
import xml.etree.ElementTree as ET
from lib.relay_transmission_error import RelayTransmissionError
import importlib


# Define global macro variables
PRG_NAM = "tdsrelay"
LOG_NAM = PRG_NAM
LOG_DIR = './logs'
LOG_FMT = '%(asctime)s - %(levelname)s - %(message)s'
LOCK_FILE = os.path.join(os.path.dirname(__file__), "{0}.lock".format(PRG_NAM))
BACKUP_FOLDER_NAME = 'transferred'
QUARANTINE = "quarantined"

class TdsRelayUnmetSpecError(Exception):
    """
    Module specific excpetion for unable to process data in unfavorable condition.
    e.g. if the .config.ini is not there, the xml file has not target node, etc.
    """
    pass


class TdsRelayError(Exception):
    """
    Module specific exception for TdsRelay class.
    """
    pass


class TdsRelay(object):
    """
    Class to conduct file forwarding from TDS to YMS.
    """

    def __init__(self, rdir, search_root=False, no_validate_customer=False, backup_when_succeed=True, all_pass=True, transfer_delay=120):
        """
        Class initializer.

        :param rdir: path to root directory which may contains multiple 'file forward directory'
        """
        self.log = logging.getLogger(__name__)
        self.root_dir = rdir
        self.config_file = ""
        self.forward_dir = ""
        self.dat_file_list = list()
        self.search_root = search_root
        self.no_validate_customer = no_validate_customer
        self.backup_when_succeed = backup_when_succeed
        self.all_pass = all_pass
        self.transfer_delay = transfer_delay

        self.ftp_conn = None  # handle for open FTP connection to YMS host
        return

    def run(self):
        """
        Perform folders globbing and then 'run_on_subfolder'
        """
        fullp = os.path.abspath(self.root_dir)
        if not os.path.exists(fullp):
            self.log.error(fullp + " does not exist")
            return

        subdirs = []
        if not self.search_root:
            subdirs = [name for name in os.listdir(fullp) if os.path.isdir(os.path.join(fullp, name))]
        else:
            subdirs = ["./"]

        for x in subdirs:
            # we don't process this folder!
            if (x.strip(r"[\/*|\\*]$") ).endswith(QUARANTINE):
                continue

            # NOTE: program run as single thread,
            # so we just change the instance variable for each sub folder
            try:
                self.forward_dir = os.path.join(fullp, x)
                self.prepare_for_subfolder()
                self.run_on_subfolder()
            except Exception as ex:
                # we leave a timestamped lock here until the .lock is removed
                # Note: this logging may be too much.  self.log.exception(ex)
                # here there is much info in diagnostic, detail log should be inside
                ##  self.log.error("Exception processing under folder {0}".format(self.forward_dir))
                continue
        return

    def prepare_for_subfolder(self):
        """
        reset to new value of instance's properties,  for working on each forward directory
        """
        self.log.info("changed forward_dir to : " + self.forward_dir)
        self.config_file = os.path.join(self.forward_dir, ".config.ini")
        self.dat_file_list.clear()

    def run_on_subfolder(self):
        """
        Main method to perform data file forwarding form TDS to YMS.

        :return: None
        """
        t0 = datetime.now()
        logfile = self.log
        cnt_fwd = 0
        # cnt_del = 0
        sections_cnt = 0
        quarantine_cnt = 0
        try_to_send_by_sections = {}
        did_sent_by_sections = {}
        quarantine_files = []
        try:

            if not os.path.exists(self.config_file):
                self.log.warning(
                    "Forward folder doesn't contains a config.ini as expected, under {0}".format(self.forward_dir))
                return
            self.get_file_list()
        except Exception as ex:
            # self.log.exception("Exception processing sub folder preparing file list :{0}".format(self.forward_dir))
            # Notes: if try to re-throw exception, please don't do the logging here! The line above
            raise TdsRelayError("Exception processing subfolder when preparing file list :{0}".format(
                self.forward_dir)) from ex

        if not self.dat_file_list:
            self.log.info("no files need transfer")
            return

        try:
            sections = self.read_config_sections()
            sections_cnt = len(sections)
            self.log.info("Found {0} FTP/SFTP connection info. {1}".format(len(sections), sections))

            shouldPostAction = False
            section_run_count = 0
            for sect in sections:
                # TODO:  it may cost to much establish/close FTP connection for each files. Consider using connection pool
                # read ini file
                section_run_count = section_run_count +1
                if section_run_count == len(sections):
                    shouldPostAction = True

                (mode, login, passwd, host, customers, dir) = self.read_config_ini(sect)
                self.ftp_conn = self.choose_transmit_mode(mode, logfile, login, passwd, host, dir)
                self.ftp_conn.ftp_open()  # open connection to YMS host
                self.log.info("Processing forwarding directory {0}".format(self.forward_dir))

                # Note: Log message that transmission failed for this file and keep going
                # For at least a few failures, this is not fatal, so we can continue, if many failures, maybe stop.

                # audit use
                if sect not in try_to_send_by_sections:
                    try_to_send_by_sections[sect] = []
                if sect not in did_sent_by_sections:
                    did_sent_by_sections[sect] = []

                # prepare to send files
                for file in self.dat_file_list:
                    try:
                        self.log.info("Forwarding file {0}".format(file))
                        try_to_send_by_sections[sect].append(file)

                        result = self.ftp_conn.ftp_upload(file)

                        if result:
                            cnt_fwd += 1
                            did_sent_by_sections[sect].append(file)

                            # Post actions
                            if shouldPostAction:
                                if self.backup_when_succeed:
                                    try:
                                        # create subfolder at current directory if not exist
                                        bak_dir = os.path.join(self.forward_dir, BACKUP_FOLDER_NAME)
                                        if not os.path.exists(bak_dir):
                                            os.makedirs(bak_dir)
                                        # move
                                        if os.path.exists(file): # potential moved due to other processing logic ,e.g. quarantine
                                            shutil.move(file, bak_dir)
                                            self.log.info("Backup the data file under {0}".format(bak_dir))
                                    except Exception as ex:
                                        self.quarantine_file(file, self.forward_dir)
                                        quarantine_files.append(file)
                                        quarantine_cnt += 1
                                        self.log.exception(ex)
                                        self.log.error("Unable to move file {0} for backup".format(file))
                                    continue
                                else:
                                    if os.path.exists(file):
                                        self.log.info("Removed data file : {0}".format(file))
                                        self.remove_file(file)  # Notes only to delete file if uploads OK
                                        
                        else:  # ftp_upload throws exception will not be caught here, we have to use result's value
                            self.quarantine_file(file, self.forward_dir)
                            quarantine_cnt += 1
                            quarantine_files.append(file)
                    except Exception as ex:
                        self.quarantine_file(file, self.forward_dir)
                        quarantine_cnt += 1
                        quarantine_files.append(file)
                        self.log.exception(ex)
                        self.log.error("Exception transferring data file  {0}".format(file))
                        continue

                    # Note: if ftp_upload raises exception, putting 'else' here is unreachable probably

        except RelayTransmissionError as ex:
            # here we handling the FTP exceptions caused by any action except the ftp_upload
            self.log.error("RelayTransmissionError can't handle error during processing data file under {0}".format(self.forward_dir))
            raise ex
        except ModuleNotFoundError as ex:
            self.log.exception(ex)
            self.log.warn("TIP: To enable SFTP mode, try 'pip install pysftp'.")
            raise TdsRelayError from ex
        except Exception as ex:
            self.log.error("Exception processing data file under {0}".format(self.forward_dir))
            self.log.exception(ex)
            raise TdsRelayError from ex
        finally:
            t1 = datetime.now()
            td = t1 - t0
            self.log.info("Processing completed in {0}".format(td))
            # Note: deleting count is not implemented
            self.log.info("For folder : {0} ".format(self.forward_dir))
            self.log.info("Total {0} files sent to quarantine : {1} ".format(quarantine_cnt, quarantine_files))
            self.log.info("{0} out of {1} (number of files: {2} ) forwarded to YMS {3} host(s)".format(cnt_fwd, sections_cnt * len(self.dat_file_list),  len(self.dat_file_list), sections_cnt))
            if sections_cnt * len(self.dat_file_list) != cnt_fwd:
                self.log.error("Missing files in transmission ...")
                sections = self.read_config_sections()
                for sect in sections:
                    diffs= diff_of_lists(try_to_send_by_sections[sect], did_sent_by_sections[sect])
                    self.log.error("For ftp/sftp connection '{0}' the missing are :  {1}".format(sect, diffs))

            self.log.info("----")
            # self.log.info("{0} files removed from forward queue".format(cnt_del))
            if self.ftp_conn:
                self.ftp_conn.ftp_close()
        return

    def get_file_list(self):

        # Loop through files in forward cache directory
        forward_list = []
        if self.all_pass:
            forward_list = glob.glob(os.path.join(self.forward_dir, "*.*"))
            forward_list = filter(lambda x: not x.endswith(".tmp"), forward_list)
        else:
            forward_list = glob.glob(os.path.join(self.forward_dir, "*.dat"))

        for file in forward_list:
            bfn = os.path.basename(file)  # base file name without path
            # only process files that are complete and static
            if self.get_file_age_seconds(file) < self.transfer_delay:
                self.log.info("Skipping over potentially changing file {0}".format(bfn))
                continue  # skip over this file go to next file

            # if original file is just a config file or non .dat file, don't process
            # This defensive measure is to protect from the glob() file_pattern was changed unexpectedly.
            if not self.all_pass and not file.endswith(".dat"):
                continue

            try:
                # we will handle exception thrown by validate_transfer_info()
                # in case there are multiple data files and others are OK to transfer
                if not self.no_validate_customer and not self.validate_transfer_info(file):
                    sections = self.read_config_sections()
                    for sect in sections:
                        (FTPmode, login, passwd, host, customers, dir) = self.read_config_ini(sect)
                        c_dat = self.get_customer_info_from_dat(file)
                        c_dat = '' if c_dat is None else str(c_dat)

                        # Note: never call remove_file here, only remove file if upload succeeds.

                        err = "Found inconsistency of customers info. in the .dat file, expect {0}, actual {1}".format(customers, c_dat)
                        raise TdsRelayUnmetSpecError(err)
            except TdsRelayUnmetSpecError as ex:
                self.log.exception(ex)
                continue

            # if we get this far in the loop, forwarded the file to YMS system for loading
            # self.log.info("Forwarding file {0}".format(bfn))
            # self.ftp_conn.ftp_upload(file)
            # cnt_fwd += 1
            self.log.debug("Adding file to file_list" + file)
            self.dat_file_list.append(file)


    def read_config_sections(self):
        """
        return sections in the .ini, each section stands for a remote FTP/SFTP connection info.
        """

        try:
            conf = configparser.ConfigParser()
            conf.read(self.config_file)
            return conf.sections()
        except Exception as ex:
            self.log.exception(ex)
            self.log.error("Exception reading config file {0}".format(self.config_file))


    def read_config_ini(self, section_name):
        try:
            conf = configparser.ConfigParser()
            conf.read(self.config_file)
            # see example .config.ini in OSAT1 directory.
            mode = conf.get(section_name, "mode")
            login = conf.get(section_name, "user")
            passwd = conf.get(section_name, "passwd")
            host = conf.get(section_name, "host")
            dir = conf.get(section_name, "outdir")
            customers = list(map(str.strip,  conf.get(section_name, "customer").split(",")))
            return (mode, login, passwd, host, customers, dir)
        except Exception as ex:
            self.log.exception(ex)
            self.log.error("Exception reading config file {0}, section {1}".format(self.config_file, section_name))

    def choose_transmit_mode(self, mode, logfile, login, passwd, host, dir):
        # Note: here we are dynamically load module using importlib and getattr,  REF: https://goo.gl/v2nyqs
        # TODO: refactor to make it more general and less code. e.g. extracting string into argument of funciton.

        if mode == "FTP":
            FtpWrapperImplClass = getattr(importlib.import_module("lib.relayftp"), "RelayFtp")
            instance = FtpWrapperImplClass(host, login, passwd, dir)
            return instance
        else:
            FtpWrapperImplClass = getattr(importlib.import_module("lib.relaysftp"), "RelaySftp")
            instance = FtpWrapperImplClass(host, login, passwd, dir)
            return instance

    def get_file_age_seconds(self, file):
        """
        Get age in seconds of last file modification.

        :param file: file being processed
        :return: age in seconds of last file modification
        """
        age = 0
        try:
            st = os.stat(file)
            age = time.time() - st.st_mtime  # file modification age in seconds
        except Exception as ex:
            self.log.exception(ex)
            self.log.error("Exception getting file modification time for {0}".format(file))
        return age

    def quarantine_file(self, file, forward_dir):
        """
        Move file to non-processing folders to deal with later

        """
        quara = os.path.join(forward_dir, QUARANTINE)
        if not os.path.exists(quara):
            os.mkdir(quara)

        if os.path.exists(file):
            self.log.info("moved file to quarantine due to exception: {0} to {1} ".format(file,quara))
            shutil.move(file, quara)


    def remove_file(self, file):
        """
        Removes specified file while logging and swallowing any exceptions.

        :param file: file being processed
        :return: None
        """
        try:
            os.unlink(file)
        except Exception as ex:
            self.log.exception(ex)
            self.log.error("Unable to remove file {0}".format(file))
        return

    def get_customer_info_from_dat(self, file):
        """
        Find the text of node <DestinationFolder> from .dat/envelope.xml file .

        :param file: file being processed
        :return: None or text of <DestinationFolder/>
        """
        try:
            z = zipfile.ZipFile(file, "r")
            x = z.open("request.xml")
            tree = ET.ElementTree(file=x)
            z.close()
            x.close()
            target_node = ".//CustomerName"
            if not any(a is None for a in [tree, tree.find(target_node)]):
                return tree.find(target_node).text

            err = "Cannot get customer info. from {0}".format(file)
            raise TdsRelayUnmetSpecError(err)
        except Exception as ex:
            raise ex
        finally:
            if z:
                z.close()
            if x:
                x.close()
        return None

    def validate_transfer_info(self, file):
        """
        check if the customer info is the same for configure and data file

        :param file: file being processed
        :return: boolean
        """

        ret = True
        c_dat = self.get_customer_info_from_dat(file)
        c_dat = '' if c_dat is None else str(c_dat)

        # check all sections!
        sections = self.read_config_sections()
        for sect in sections:
            (FTPmode, login, passwd, host, customers, dir) = self.read_config_ini(sect)
            self.log.info("Validate config, customer from  section {0} of .ini : {1} , from .dat : {2} ".format(sect, customers, c_dat ))
            if c_dat not in customer:
                return False
        return ret


def get_log_name():
    """
    Returns name for a daily application log file in form 'some-obfuscator-YYYY-MM-DD.log'.

    :return: application log file name with date/time stamp
    """
    if not os.path.isdir(LOG_DIR):
        raise TdsRelayError("Default log directory does not exist: {0}".format(LOG_DIR))
    local = datetime.now()
    logname = "{0}-{1}.log".format(LOG_NAM, local.strftime("%Y-%m-%d"))
    return os.path.join(LOG_DIR, logname)


def create_log_dir():
    """
    Create a folder named 'logs' under current directory if not exists

    :return: None
    """
    logdir = os.path.abspath(LOG_DIR)
    if not os.path.exists(logdir):
        os.makedirs(logdir)


def parse_args():
    """
    Parses command line arguments into an argument set.

    :return: command line argument set
    """
    cl = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description="{0}: TDS to YMS data file forwarder.".format(PRG_NAM),
                                 epilog="Usage example:\n"
                                        "python3 {0}.py -r D:\Exensio\TDS-OEE\Files\Forward\n"
					"python3 {1}.py -r ./tests/data --search-root --backup --all-pass --delay=0".format(PRG_NAM, PRG_NAM))
    cl.add_argument("-r", "--root-dir", dest="rdir", action="store", required=True,
                    help="TDS Loader Root directory")
    cl.add_argument("--search-root", dest="is_search_root", action="store_true", required=False,
                    help="TDS relay shall glob .dat files just under the root dir,default:false")
    cl.add_argument("--no-validate-customer", dest="is_no_validate_customer",  action="store_true", required=False,
                    help="TDS relay shall not validate .dat with .ini for customer information, false if not specified")
    cl.add_argument("--backup", dest="is_backup_when_succeed",  action="store_false", required=False,
                    help="TDS relay shall remove files after successful transferring, default:false. Otherwise saved to ./transferred")
    cl.add_argument("--all-pass", dest="is_all_pass",  action="store_true", required=False,
                    help="TDS relay shall send all files, default:false")
    cl.add_argument("--delay", dest="transfer_delay",  nargs='?', const=120, type=int, required=False, default=120,
                    help="TDS relay shall delay transfer in second, default:120s")

    args = cl.parse_args()
    return args


def get_lock_or_exit(log):
    """
    Get a runtime lock by creating a semaphore file, or exit.
    This mechanism prevents overlapping execution for a process that might be long running.

    :param log: handle to application log
    :return: None
    """
    if os.path.isfile(LOCK_FILE):
        log.warning("Another instance of this utility is running or a deadlock has occurred.")
        log.warning("If certain that a deadlock has occurred, remove semaphore file: {0}".format(LOCK_FILE))
        sys.exit(0)
    # write running semaphore file with date/time of startup
    with open(LOCK_FILE, 'w', newline='\r\n') as f:
        f.write("{0} {1}\n".format(__file__, datetime.now().isoformat()))
        log.info("Created lock semaphore file {0}".format(LOCK_FILE))
    return


def release_lock(log):
    """
    Releases the runtime lock by removing the semaphore file.

    :param log: handle to application log
    """
    if os.path.isfile(LOCK_FILE):
        log.info("Removing lock semaphore file {0}".format(LOCK_FILE))
        os.remove(LOCK_FILE)
    return


def main():
    """
    Main method for utility to convert PDL files to TXT files.
    """
    create_log_dir()
    logfile = get_log_name()
    logging.basicConfig(filename=logfile, level=logging.INFO, format=LOG_FMT)
    log = logging.getLogger(__name__)

    dtstr = datetime.now().isoformat()[:-7]
    log.info("***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***")
    log.info("***  {0}.py (version {1}) -- Launched {2}".format(PRG_NAM, __version__, dtstr))
    log.info("***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***")
    get_lock_or_exit(log)

    try:
        args = parse_args()
        log.info("is_search_root".ljust(50) + ("YES" if args.is_search_root  else "NO") )
        log.info("is_no_validate_customer".ljust(50) + ("YES" if args.is_no_validate_customer  else "NO") )
        log.info("is_backup_when_succeed".ljust(50) + ("YES" if args.is_search_root  else "NO") )
        log.info("is_all_pass".ljust(50) + ("YES" if args.is_all_pass  else "NO") )
        log.info("transfer_delay".ljust(50) + str(args.transfer_delay) + " seconds" )
        forwarder = TdsRelay(args.rdir,
                             args.is_search_root,
                             args.is_no_validate_customer,
                             args.is_backup_when_succeed,
                             args.is_all_pass,
                             args.transfer_delay)
        forwarder.run()
    except Exception as ex:
        log.error(ex)

    finally:
        release_lock(log)
        log.info("\nSee log file for details ({0}).\n".format(logfile))
        dtstr = datetime.now().isoformat()[:-7]
        log.info("***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***")
        log.info("***  {0}.py (version {1}) -- Terminated {2}".format(PRG_NAM, __version__, dtstr))
        log.info("***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***  ***")
        logging.shutdown()

        sys.exit()

# Python code t get difference of two lists
# Not using set()
def diff_of_lists(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif


# Python boilerplate idiom to call the main() function.
if __name__ == '__main__':
    main()
