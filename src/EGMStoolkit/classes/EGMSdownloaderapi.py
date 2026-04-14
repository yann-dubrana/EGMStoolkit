#! /usr/bin/env python3
# -*- coding: iso-8859-1 -*-

"""
Classe compoment of **EGMS toolkit**

The module contains the classe and the methods to download the tile regarding a user ROI, required by to run `EGMStoolkit`.

    (From `EGMStoolkit` package)

Changelog:
    * 0.3.0: Delete the support of wget, Alexis Hrysiewicz, Oct. 2025
    * 0.2.15: Add the possibility to unzip files in parallel, Alexis Hrysiewicz, Apr. 2025
    * 0.2.12: Add the support of the 2019_2023 release, Nov. 2024, Alexis Hrysiewicz
    * 0.2.0: Script structuring, Jan. 2024, Alexis Hrysiewicz
    * 0.1.0: Initial version, Nov. 2023

"""

import os
import zipfile
import numpy as np
import glob
import shutil
from typing import Optional, Union
from joblib import Parallel, delayed
import time
import threading
import requests
import re

from EGMStoolkit.functions import egmsapitools
from EGMStoolkit import usermessage
from EGMStoolkit import constants

################################################################################
## Module-level helpers for parallel downloads
################################################################################
class _ProgressCounter:
    """Thread-safe counter that prints progress from parallel workers."""

    def __init__(self, total, log):
        self._lock = threading.Lock()
        self._done = 0
        self.total = total
        self.log = log

    def increment(self, filename_label, status_char):
        """Increment and print one progress line.

        status_char: '+' = downloaded, '=' = already done, 'x' = failed/skipped
        """
        with self._lock:
            self._done += 1
            done = self._done
        usermessage.egmstoolkitprint(
            '[%s] %d / %d : %s' % (status_char, done, self.total, filename_label),
            self.log, True
        )


class _RateLimiter:
    """Shared cooldown timer across parallel workers.

    When any worker hits 429/502, it records a cooldown deadline.
    All workers check this before firing a request and sleep until
    the deadline passes. No blocking Event — just a shared timestamp
    protected by a lock.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._resume_at = 0.0  # epoch seconds

    def signal(self, wait_seconds):
        """Record that requests should pause for wait_seconds from now."""
        deadline = time.monotonic() + wait_seconds
        with self._lock:
            if deadline > self._resume_at:
                self._resume_at = deadline

    def wait_if_needed(self):
        """Sleep until the cooldown deadline, if any."""
        with self._lock:
            remaining = self._resume_at - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)


def _try_url(url, output_file_path, rate_limiter, log, verbose_worker, max_retries, retry_wait):
    """Attempt to download one URL with retry logic.

    Returns:
        'ok'       — file written successfully
        'exists'   — 416, already complete
        'ratelimit'— exhausted retries on 429/502
        'error'    — non-retryable HTTP error
    """
    for attempt in range(max_retries):
        if rate_limiter is not None:
            rate_limiter.wait_if_needed()

        existing_size = os.path.getsize(output_file_path) if os.path.exists(output_file_path) else 0
        try:
            response = requests.get(
                url,
                headers={'Range': 'bytes=%d-' % existing_size},
                stream=True,
                allow_redirects=True,
                timeout=(5, 5)
            )
        except Exception as e:
            usermessage.egmstoolkitprint(
                'Connection error (attempt %d/%d): %s' % (attempt + 1, max_retries, e),
                log, verbose_worker
            )
            wait_time = min(retry_wait * (2 ** attempt), 60)
            if rate_limiter is not None:
                rate_limiter.signal(wait_time)
            else:
                time.sleep(wait_time)
            continue

        status = response.status_code

        if status == 416:
            return 'exists'

        if status in (429, 502):
            wait_time = min(retry_wait * (2 ** attempt), 300)
            label = '429 Too Many Requests' if status == 429 else '502 Bad Gateway'
            usermessage.egmstoolkitprint(
                'EGMS-toolkit - Downloader - %s. Cooling down %ds (attempt %d/%d)' % (
                    label, wait_time, attempt + 1, max_retries),
                log, verbose_worker
            )
            if rate_limiter is not None:
                rate_limiter.signal(wait_time)
                rate_limiter.wait_if_needed()
            else:
                time.sleep(wait_time)
            continue

        if status not in (200, 206):
            return 'error'

        mode = 'ab' if existing_size > 0 else 'wb'
        with open(output_file_path, mode) as f:
            for chunk in response.iter_content(chunk_size=constants.__chunksize__):
                if chunk:
                    f.write(chunk)
        return 'ok'

    return 'ratelimit'


def _download_one_file(work_item, log, verbose_worker, rate_limiter=None,
                       max_retries=4, retry_wait=5, progress=None):
    """Download a single EGMS file with shared rate-limit awareness.
    If the primary URL fails with 502/ratelimit (file not found), alternate
    version suffixes _2 through _9 are tried (2 attempts each).

    Args:
        work_item: tuple (url_base, output_file_path, filename_label, token)

    Returns:
        tuple: (filename_label, success, error_msg)
    """
    url_base, output_file_path, filename_label, token = work_item

    def make_url(base, tok):
        return '%s?id=%s' % (base, tok)

    # Build candidate URL list: primary first, then _2.._9 variants
    def alternate_urls(base_url):
        """Yield (url, label) for primary and version-suffix alternates."""
        yield base_url, filename_label
        # Replace trailing version digit before .zip: _2019_2023_1.zip → _2019_2023_2.zip
        for v in range(2, 10):
            alt = re.sub(r'(_\d+)\.zip$', '_%d.zip' % v, base_url)
            if alt != base_url:
                alt_label = re.sub(r'(_\d+)\.zip$', '_%d.zip' % v, filename_label)
                yield alt, alt_label
            else:
                break   # filename has no version suffix to vary

    for url_base_candidate, label_candidate in alternate_urls(url_base):
        full_url = make_url(url_base_candidate, token)
        usermessage.egmstoolkitprint(
            'EGMS-toolkit - Downloader - Trying: %s' % label_candidate,
            log, verbose_worker
        )
        result = _try_url(full_url, output_file_path, rate_limiter,
                          log, verbose_worker, max_retries, retry_wait)

        if result == 'ok':
            usermessage.egmstoolkitprint(
                'EGMS-toolkit - Downloader - Download complete: %s' % label_candidate,
                log, verbose_worker
            )
            if progress is not None:
                progress.increment(label_candidate, '+')
            return (label_candidate, True, None)

        if result == 'exists':
            if progress is not None:
                progress.increment(label_candidate, '=')
            return (label_candidate, True, None)

        if result == 'error':
            # Non-retryable HTTP error — stop trying alternates
            if progress is not None:
                progress.increment(label_candidate, 'x')
            return (label_candidate, False, 'HTTP error on %s' % label_candidate)

        # result == 'ratelimit' → 502/429 exhausted → try next suffix
        usermessage.egmstoolkitprint(
            'EGMS-toolkit - Downloader - Not found: %s, trying next suffix...' % label_candidate,
            log, verbose_worker
        )

    # All suffixes exhausted
    if not os.path.isfile(output_file_path):
        if progress is not None:
            progress.increment(filename_label, 'x')
        return (filename_label, False, 'file not available on server after trying all suffixes')
    if progress is not None:
        progress.increment(filename_label, 'x')
    return (filename_label, False, 'max retries exceeded')



################################################################################
## Creation of a class to manage the Sentinel-1 burst ID map
################################################################################
class egmsdownloader:
    """`egmsdownloader` class.
        
    Attributes:

        listL2a (list): Storage of available data [Default: empty]
        listL2alink (list): Storage of available data [Default: empty]
        listL2b (list): Storage of available data [Default: empty]
        listL2blink (list): Storage of available data [Default: empty]
        listL3UD (list): Storage of available data [Default: empty]
        listL3UDlink (list): Storage of available data [Default: empty]
        listL3EW (list): Storage of available data [Default: empty]
        listL3EWlink (list): Storage of available data [Default: empty]
        token (str): User token [Default: 'XXXXXXX--XXXXXXX']            
        verbose (bool): Verbose [Default: `True`]
        log (str or None): Loggin mode [Default: `None`]

    """ 

    ################################################################################
    ## Initialistion of the class
    ################################################################################
    def __init__(self, 
        listL2a: Optional[any] = [],        
        listL2alink: Optional[any] = [],
        listL2b: Optional[any] = [],
        listL2blink: Optional[any] = [],
        listL3UD: Optional[any] = [],
        listL3UDlink: Optional[any] = [],
        listL3EW: Optional[any] = [],
        listL3EWlink: Optional[any] = [],
        token: Optional[str] = 'XXXXXXX--XXXXXXX',
        verbose: Optional[bool] = True,
        log: Optional[Union[str, None]] = None): 
        """`egmsdownloader` initialisation.
        
        Args:

            listL2a (list, Optional): Storage of available data [Default: empty]
            listL2alink (list, Optional): Storage of available data [Default: empty]
            listL2b (list, Optional): Storage of available data [Default: empty]
            listL2blink (list, Optional): Storage of available data [Default: empty]
            listL3UD (list, Optional): Storage of available data [Default: empty]
            listL3UDlink (list, Optional): Storage of available data [Default: empty]
            listL3EW (list, Optional): Storage of available data [Default: empty]
            listL3EWlink (list, Optional): Storage of available data [Default: empty]
            token (str, Optional): User token [Default: 'XXXXXXX--XXXXXXX']            
            verbose (bool, Optional): Verbose [Default: `True`]
            log (str or None, Optional): Loggin mode [Default: `None`]

        Return `egmsdownloader` class

        """ 
        
        self.listL2a = listL2a
        self.listL2alink = listL2alink
        self.listL2b = listL2b
        self.listL2blink = listL2blink
        self.listL3UD = listL3UD
        self.listL3UDlink = listL3UDlink
        self.listL3EW = listL3EW
        self.listL3EWlink = listL3EWlink

        if isinstance(token, list):
            self.tokens = [str(t).strip() for t in token]
        elif isinstance(token, str) and ',' in token:
            self.tokens = [t.strip() for t in token.split(',')]
        else:
            self.tokens = [str(token).strip()]

        self.verbose = verbose
        self.log = log
        self.missing = []
        self.failed = []
        self.downloaded = []

        self.checkparameter(verbose=False)

    @property
    def token(self):
        """First token (backward compatibility)."""
        return self.tokens[0] if self.tokens else 'XXXXXXX--XXXXXXX'

    @token.setter
    def token(self, value):
        if isinstance(value, list):
            self.tokens = [str(v).strip() for v in value]
        else:
            self.tokens = [str(value).strip()]

    ################################################################################
    ## Check parameters
    ################################################################################
    def checkparameter(self, verbose: Optional[Union[bool,None]] = None):
        """Check the parameter
        
        Args:

            verbose (bool or None, Optional): Verbose if `None`, use the verbose mode of the job [Default: `None`]

        Return

            `egmsdownloader` class

        """ 

        if verbose == None:
            verbose = self.verbose
        if not isinstance(verbose,bool):
            raise ValueError(usermessage.errormsg(__name__,'checkparameter',__file__,constants.__copyright__,'Verbose must be True or False',self.log))
        
        usermessage.openingmsg(__name__,'checkparameter',__file__,constants.__copyright__,'Check the parameter',self.log,verbose)

        for tok in self.tokens:
            if tok == 'XXXXXXX--XXXXXXX':
                usermessage.warningmsg(__name__,'checkparameter',__file__,'One or more user tokens are not correct.',self.log,True)
                break

    ################################################################################
    ## Function to print the attributes
    ################################################################################
    def print(self):
        """Print the class attributes

        Return 

            `egmsdownloader` class

        """ 

        attrs = vars(self)
        print(', '.join("%s: %s" % item for item in attrs.items()))

        return self

    ################################################################################
    ## Function to update ethe list of files
    ################################################################################
    def updatelist(self,infoS1ROIparameter, verbose: Optional[Union[bool,None]] = None):
        """Update the list of of EGMS files
        
        Args:

            infoS1ROIparameter: `S1ROIparameter` class
            verbose (bool or None, Optional): Verbose if `None`, use the verbose mode of the job [Default: `None`]

        Return

            `egmsdownloader` class

        """ 

        if verbose == None:
            verbose = self.verbose
        if not isinstance(verbose,bool):
            raise ValueError(usermessage.errormsg(__name__,'updatelist',__file__,constants.__copyright__,'Verbose must be True or False',self.log))
        
        self.checkparameter(verbose = False)

        usermessage.openingmsg(__name__,__name__,__file__,constants.__copyright__,'Update the list of of EGMS files',self.log,verbose)

        release_para = egmsapitools.check_release(infoS1ROIparameter.release)

        if infoS1ROIparameter.egmsL3component == 'UD': 
            ext_3D = 'U'
        elif infoS1ROIparameter.egmsL3component == 'EW': 
            ext_3D = 'E'
        
        if infoS1ROIparameter.Data: 
            if infoS1ROIparameter.egmslevel == 'L2a' or infoS1ROIparameter.egmslevel == 'L2b': 
                for tracki in infoS1ROIparameter.Data:
                    for idx in ['1','2','3']: 
                        for iwi in infoS1ROIparameter.Data[tracki]['IW%s' %(idx)]:
                            name_zip = 'EGMS_%s_%03d_%04d_IW%s_VV%s.zip' % (infoS1ROIparameter.egmslevel,iwi['relative_orbit_number'],iwi['egms_burst_id'],idx,release_para[1])
                            link_zip = 'https://egms.land.copernicus.eu/insar-api/archive/download/%s' % (name_zip)
                            if infoS1ROIparameter.egmslevel == 'L2a':
                                self.listL2a.append(name_zip)
                                self.listL2alink.append(link_zip)
                            elif infoS1ROIparameter.egmslevel == 'L2b':
                                self.listL2b.append(name_zip)
                                self.listL2blink.append(link_zip)

        if infoS1ROIparameter.DataL3:
            if infoS1ROIparameter.egmslevel == 'L3':
                for tilei in infoS1ROIparameter.DataL3['polyL3']:
                
                    x = tilei.exterior.coords.xy[0].tolist()[0]/100000
                    y = tilei.exterior.coords.xy[1].tolist()[0]/100000

                    name_zip = 'EGMS_L3_E%2dN%2d_100km_%s%s.zip' % (y,x,ext_3D,release_para[1])
                    link_zip = 'https://egms.land.copernicus.eu/insar-api/archive/download/%s' % (name_zip)

                    if infoS1ROIparameter.egmsL3component == 'UD':
                        self.listL3UD.append(name_zip)
                        self.listL3UDlink.append(link_zip)
                    elif infoS1ROIparameter.egmsL3component == 'EW':
                        self.listL3EW.append(name_zip)
                        self.listL3EWlink.append(link_zip)

        self.listL2a = np.unique(self.listL2a).tolist()
        self.listL2alink = np.unique(self.listL2alink).tolist()
        self.listL2b = np.unique(self.listL2b).tolist()
        self.listL2blink = np.unique(self.listL2blink).tolist()
        self.listL3UD = np.unique(self.listL3UD).tolist()
        self.listL3UDlink = np.unique(self.listL3UDlink).tolist()
        self.listL3EW = np.unique(self.listL3EW).tolist()
        self.listL3EWlink = np.unique(self.listL3EWlink).tolist()


        self.printlist(verbose=verbose)

        return self

    ################################################################################
    ## Function to print the list(s) of files
    ################################################################################
    def printlist(self, 
        verbose: Optional[Union[bool,None]] = None):
        """Print the list(s) of EGMS files
        
        Args:

            verbose (bool or None, Optional): Verbose if `None`, use the verbose mode of the job [Default: `None`]

        Return

            `egmsdownloader` class

        """ 

        if verbose == None:
            verbose = self.verbose
        if not isinstance(verbose,bool):
            raise ValueError(usermessage.errormsg(__name__,'updatelist',__file__,constants.__copyright__,'Verbose must be True or False',self.log))
        
        self.checkparameter(verbose = False)

        usermessage.openingmsg(__name__,__name__,__file__,constants.__copyright__,'Print the list(s) of EGMS files',self.log,verbose)

        for type in ['L2a', 'L2b', 'L3UD', 'L3EW']:
            datatmp = eval('self.list%s' % (type))
            datatmplink = eval('self.list%slink' % (type))
        
            if datatmp: 
                usermessage.egmstoolkitprint('For the EGMS data: %s' % (type),self.log,verbose)
                for idx in np.arange(len(datatmp)): 
                    release_para = egmsapitools.check_release_fromfile(datatmp[idx])
                    usermessage.egmstoolkitprint('\t File %d: %s (Release %s)' % (idx+1,datatmp[idx],release_para[0]),self.log,verbose)

        return self

    ################################################################################
    ## Function to download the files
    ################################################################################
    def download(self,
        outputdir: Optional[str] = '.%sOutput' % (os.sep),
        unzipmode: Optional[bool] = False,
        cleanmode: Optional[bool] = False,
        force: Optional[bool] = True,
        nbworker: Optional[int] = 1,
        verbose: Optional[Union[bool,None]] = None):
        """Download the EGMS files

        Args:

            outputdir (str, Optional): Path of the output directory [Default: './Output']
            unzipmode (bool, Optional): Unzip the file [Default: `False`]
            cleanmode (bool, Optional): Delete the file after unzipping [Default: `False`]
            force (bool, Optional): Replace the stored file [Default: `True`]
            nbworker (int, Optional): Number of parallel download workers, 1–8 [Default: 1]
            verbose (bool or None, Optional): Verbose if `None`, use the verbose mode of the job [Default: `None`]

        Return

            `egmsdownloader` class

        """

        if verbose == None:
            verbose = self.verbose
        if not isinstance(verbose, bool):
            raise ValueError(usermessage.errormsg(__name__,'download',__file__,constants.__copyright__,'Verbose must be True or False',self.log))

        if not isinstance(nbworker, int) or nbworker < 1 or nbworker > 8:
            raise ValueError(usermessage.errormsg(__name__,'download',__file__,constants.__copyright__,'nbworker must be an integer between 1 and 8.',self.log))

        self.checkparameter(verbose=False)

        usermessage.openingmsg(__name__,__name__,__file__,constants.__copyright__,'Download the EGMS files',self.log,verbose)

        os.makedirs(outputdir, exist_ok=True)

        # Phase 1: pre-create all directories to avoid race conditions in parallel mode
        for type_label in ['L2a', 'L2b', 'L3UD', 'L3EW']:
            datatmp = getattr(self, 'list%s' % type_label)
            if datatmp:
                type_dir = os.path.join(outputdir, type_label)
                os.makedirs(type_dir, exist_ok=True)
                for filename in datatmp:
                    release_para = egmsapitools.check_release_fromfile(filename)
                    pathdir = os.path.join(type_dir, release_para[0])
                    os.makedirs(pathdir, exist_ok=True)

        # Phase 2: build flat work list with round-robin token assignment
        work_items = []
        for type_label in ['L2a', 'L2b', 'L3UD', 'L3EW']:
            datatmp = getattr(self, 'list%s' % type_label)
            datatmplink = getattr(self, 'list%slink' % type_label)
            if datatmp:
                type_dir = os.path.join(outputdir, type_label)
                for filename, link in zip(datatmp, datatmplink):
                    release_para = egmsapitools.check_release_fromfile(filename)
                    pathdir = os.path.join(type_dir, release_para[0])
                    out_path = os.path.join(pathdir, link.split('/')[-1])
                    if not os.path.isfile(out_path) or force:
                        token = self.tokens[len(work_items) % len(self.tokens)]
                        work_items.append((link, out_path, filename, token))
                    else:
                        usermessage.egmstoolkitprint(
                            '\tAlready downloaded (detection of the .zip file): %s' % filename,
                            self.log, verbose
                        )

        # Phase 3: dispatch downloads (sequential or parallel)
        total_len = len(work_items)
        verbose_worker = verbose if nbworker == 1 else False

        rate_limiter = _RateLimiter()
        progress = _ProgressCounter(total_len, self.log)

        if nbworker == 1:
            results = []
            for h, item in enumerate(work_items, 1):
                usermessage.egmstoolkitprint(
                    '%d / %d files: Download the file: %s' % (h, total_len, item[2]),
                    self.log, verbose
                )
                results.append(_download_one_file(item, self.log, verbose_worker,
                                                   rate_limiter=rate_limiter,
                                                   progress=None))  # sequential: header line is enough
        else:
            usermessage.egmstoolkitprint(
                'Downloading %d files with %d workers (%d token(s))' % (total_len, nbworker, len(self.tokens)),
                self.log, verbose
            )
            results = Parallel(n_jobs=nbworker, backend='threading')(
                delayed(_download_one_file)(item, self.log, False,
                                            rate_limiter=rate_limiter,
                                            progress=progress)
                for item in work_items
            )

        not_available = []
        failed = []
        downloaded = []
        for filename_label, success, error_msg in results:
            if not success:
                if error_msg and 'not available on server' in error_msg:
                    not_available.append(filename_label)
                    usermessage.egmstoolkitprint(
                        'Skipped (not available on server): %s' % filename_label,
                        self.log, verbose
                    )
                else:
                    failed.append(filename_label)
                    usermessage.egmstoolkitprint(
                        'Failed to download %s: %s' % (filename_label, error_msg),
                        self.log, verbose
                    )
            else:
                downloaded.append(filename_label)

        if not_available:
            usermessage.egmstoolkitprint(
                '%d file(s) not available on server: %s' % (len(not_available), ', '.join(not_available)),
                self.log, verbose
            )
        if failed:
            usermessage.egmstoolkitprint(
                '%d file(s) failed to download (check log for details): %s' % (len(failed), ', '.join(failed)),
                self.log, verbose
            )

        # Store for post-download map
        self.missing = not_available
        self.failed = failed
        self.downloaded = downloaded

        self.unzipfile(
            outputdir=outputdir,
            unzipmode=unzipmode,
            cleanmode=cleanmode,
            verbose=verbose
        )

        return self
    ################################################################################
    ## Function to unzip the files
    ################################################################################
    def unzipfile(self,
        outputdir: Optional[str] = '.%sOutput' % (os.sep),     
        unzipmode: Optional[bool] = True,
        nbworker: Optional[int] = 1,
        cleanmode: Optional[bool] = False,        
        verbose: Optional[Union[bool,None]] = None):
        """Unzip the EGMS files
        
        Args:

            outputdir (str, Optional): Path of the output directory [Default: './Output']
            unzipmode (bool, Optional): Unzip the file [Default: `True`]
            nbworker (int, Optional): Number of workers for unzipping [Default: 1]
            cleanmode (bool, Optional): Delete the file after unzipping [Default: `False`]
            verbose (bool or None, Optional): Verbose if `None`, use the verbose mode of the job [Default: `None`]

        Return

            `egmsdownloader` class

        """ 

        if verbose == None:
            verbose = self.verbose
        if not isinstance(verbose,bool):
            raise ValueError(usermessage.errormsg(__name__,'unzipfile',__file__,constants.__copyright__,'Verbose must be True or False',self.log))
        
        if not isinstance(nbworker,int):
            raise ValueError(usermessage.errormsg(__name__,'nbworker',__file__,constants.__copyright__,'nbworker must be an int.',self.log))
        else: 
            if nbworker < 1: 
                raise ValueError(usermessage.errormsg(__name__,'nbworker',__file__,constants.__copyright__,'nbworker must be >= 1.',self.log))

        self.checkparameter(verbose = False)

        usermessage.openingmsg(__name__,__name__,__file__,constants.__copyright__,'Unzip the EGMS files',self.log,verbose)

        list_files = glob.glob('%s%s*%s*%s*.zip' % (outputdir,os.sep,os.sep,os.sep))

        ####################
        def unziponefile(fi,cleanmode,h): 
            pathsplit = fi.split(os.sep)
            namefile = fi.split(os.sep)[-1].split('.')[0]
            pathdirfile = ''
            for i1 in np.arange(len(pathsplit)-1):
                if i1 == 0:
                    pathdirfile = pathsplit[i1] 
                else:
                    pathdirfile = pathdirfile + os.sep + pathsplit[i1]     

            if not h == None:           
                usermessage.egmstoolkitprint('%d / %d files: Unzip the file: %s' % (h,len(list_files),pathsplit[-1]),self.log,verbose)
            else: 
                usermessage.egmstoolkitprint('Unzip the file: %s' % (pathsplit[-1]),self.log,verbose)

            with zipfile.ZipFile("%s" %(fi), 'r') as zip_ref:
                zip_ref.extractall('%s%s%s' % (pathdirfile,os.sep,namefile))

            if os.path.isdir('%s%s%s' % (pathdirfile,os.sep,namefile)) and (cleanmode): 
                os.remove(fi)
        ####################

        if unzipmode:
            if nbworker == 1:
                h = 1
                for fi in list_files:
                    unziponefile(fi,cleanmode,h)
                    h += 1
            else: 
                usermessage.egmstoolkitprint('Unzipping with %s workers' % (nbworker),self.log,verbose)  
                Parallel(n_jobs=nbworker)(delayed(unziponefile)(fi,cleanmode,None) for fi in list_files)
        else: 
                usermessage.egmstoolkitprint('\tNo processing.',self.log,verbose)  

        return self

    ################################################################################
    ## Function to clean the unused files
    ################################################################################
    def clean(self,
        outputdir: Optional[str] = '.%sOutput' % (os.sep),     
        verbose: Optional[Union[bool,None]] = None):
        """Clean the unused files (based on the list(s))
        
        Args:

            outputdir (str, Optional): Path of the output directory [Default: './Output']
            verbose (bool or None, Optional): Verbose if `None`, use the verbose mode of the job [Default: `None`]

        Return

            `egmsdownloader` class

        """ 

        if verbose == None:
            verbose = self.verbose
        if not isinstance(verbose,bool):
            raise ValueError(usermessage.errormsg(__name__,'clean',__file__,constants.__copyright__,'Verbose must be True or False',self.log))
        
        self.checkparameter(verbose = False)

        usermessage.openingmsg(__name__,__name__,__file__,constants.__copyright__,'Clean the unused files (based on the list(s))',self.log,verbose)

        if not os.path.isdir(outputdir): 
            raise ValueError(usermessage.errormsg(__name__,'clean',__file__,constants.__copyright__,'Impossible to find the output directory',self.log))

        listdirall = []
        listfileall = []
        for type in ['L2a', 'L2b', 'L3UD', 'L3EW']:
            datatmp = eval('self.list%s' % (type))
            if datatmp: 
                for idx in np.arange(len(datatmp)): 
                    release_para = egmsapitools.check_release_fromfile(datatmp[idx])
                    listdirall.append('%s%s%s%s%s%s%s' % (outputdir,os.sep,type,os.sep,release_para[0],os.sep,datatmp[idx].split('.')[0]))
                    listfileall.append('%s%s%s%s%s%s%s' % (outputdir,os.sep,type,os.sep,release_para[0],os.sep,datatmp[idx]))

        liststored = glob.glob('%s%s*%s*%s*' % (outputdir,os.sep,os.sep,os.sep))
        liststoredDIR = []
        liststoredFILE = []
        for li in liststored: 
            if os.path.isfile(li):
                liststoredFILE.append(li)
            else: 
                liststoredDIR.append(li)

        for li in liststoredDIR: 
            if not li in listdirall:
                usermessage.egmstoolkitprint('The directory %s is not in the list(s), it will be removed...' % (li),self.log,verbose)  
                shutil.rmtree(li)
            else:
                usermessage.egmstoolkitprint('The directory %s is in the list(s), it will be kept...' % (li),self.log,verbose)  

        for li in liststoredFILE: 
            if not li in listfileall: 
                usermessage.egmstoolkitprint('The .zip file %s is not in the list(s), it will be removed...' % (li),self.log,verbose)  
                os.remove(li)
            else:
                usermessage.egmstoolkitprint('The .zip file %s is in the list(s), it will be kept...' % (li),self.log,verbose)  

        #  Clean the empty directories 
        for i1 in ['L2a', 'L2b', 'L3UD', 'L3EW']:
            for i2 in ['2015_2021', '2018_2022', '2019_2023']:
                if os.path.isdir('%s%s%s%s%s' % (outputdir,os.sep,i1,os.sep,i2)):
                    if len(os.listdir('%s%s%s%s%s' % (outputdir,os.sep,i1,os.sep,i2))) == 0: 
                        shutil.rmtree('%s%s%s%s%s' % (outputdir,os.sep,i1,os.sep,i2))

            if os.path.isdir('%s%s%s' % (outputdir,os.sep,i1)):
                if len(os.listdir('%s%s%s' % (outputdir,os.sep,i1))) == 0: 
                    shutil.rmtree('%s%s%s' % (outputdir,os.sep,i1))
        
        return self
