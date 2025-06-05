#!/usr/bin/env python
#-*-coding:utf8-*-
from __future__ import unicode_literals
import smbclient
import locale
import subprocess
import logging
import os
import six
import re
mylog = logging.getLogger(__name__)

smbclient._file_re = re.compile(r"""
\s{2}               # file lines start with 2 spaces
(.*?)\s+            # capture filename non-greedy, eating remaining spaces
([ADHSRntc]*)          # capture file mode
\s+                 # after the mode you can have any number of spaces
(\d+)               # file size
\s+                 # spaces after file size
(                   # begin date capturing
    \w{3}               # abbrev weekday
    \s                  # space
    \w{3}               # abbrev month
    \s{1,2}             # one or two spaces before the day
    \d{1,2}             # day
    \s                  # a space before the time
    \d{2}:\d{2}:\d{2}   # time
    \s                  # space
    \d{4}               # year
)                   # end date capturing
$                   # end of string""", re.VERBOSE)


class PatchedSmbClient(smbclient.SambaClient):

    def __init__(self, **kwargs):
        super(PatchedSmbClient, self).__init__(**kwargs)
        self.password = kwargs.get('password', '')
        self.username = kwargs.get('username', '')
        self.runcmd_num_of_attemps = 1
        #if '-N' not in self._smbclient_cmd:
        #    self._smbclient_cmd.insert(1, '-N')
        if '-U' not in self._smbclient_cmd:
            self._smbclient_cmd.insert(1, "'{}%{}'".format(self.username, self.password))
            self._smbclient_cmd.insert(1, '-U')
        for i in range(len(self._smbclient_cmd)):
            if not six.PY2:
                if type(self._smbclient_cmd[i]) is bytes:
                    self._smbclient_cmd[i] = self._smbclient_cmd[i].decode('utf-8')

    def _raw_runcmd(self, command):
        cmd = self._smbclient_cmd + ['-c', "timeout 120; iosize 16384; {}'".format(command)]
        # cmd = self._smbclient_cmd + ['-c', "'{}'".format(command).encode('utf-8')]
        strcmd = " ".join(map(lambda x: str(x), cmd))
        mylog.debug(strcmd)
        #p = subprocess.Popen(args=cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        result = ''
        returncode = 0
        if six.PY2:
            try:
                result = subprocess.check_output(args=strcmd, stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                returncode = e.returncode
                result = e.output
        else:
            p = subprocess.run(args=strcmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            result = p.stdout.decode('utf-8')
            returncode = p.returncode

        self._runcmd_attemps = 1
        if returncode != 0 and self.runcmd_num_of_attemps:
            if self._runcmd_attemps < self.runcmd_num_of_attemps:
                self._runcmd_attemps += 1
                mylog.warning(u"Number of attemps: %s (cmd: %s)" % (self._runcmd_attemps, cmd))
                return self._raw_runcmd(command)
            raise smbclient.SambaClientError("Error on %r: %r" % (' '.join(cmd), result))
        self._runcmd_attemps = 1
        if six.PY2:
            #mylog.debug(result.decode('utf-8'))
            pass
        else:
            pass
            #mylog.debug(result)
        return result

    def download(self, remote_path, local_path):
        result = super(PatchedSmbClient, self).download(remote_path, local_path)
        if not os.path.exists(local_path):
            mylog.warning("file not downloaded")
        return result

    def lsdir(self, path, ext='*'):
        """
        Lists a directory
        returns a list of tuples in the format:
        [(filename, modes, size, date), ...]
        """
        path = os.path.join(path, ext)
        return self.glob(path)

    def glob(self, path):
        """
        Lists a glob (example: "/files/somefile.*")
        returns a list of tuples in the format:
        [(filename, modes, size, date), ...]
        \s{2}(.*?)\s+([ADHSR]*)\s+(\d+)\s+(\w{3}\s\w{3}\s{1,2}\d{1,2}\s\d{2}:\d{2}:\d{2}\s\d{4})
        """
        files = self._runcmd(u'ls', path).splitlines()
        for filedata in files:
            mylog.debug(filedata)
            m = smbclient._file_re.match(filedata)
            if m:
                name, modes, size, date = m.groups()
                if name == '.' or name == '..':
                    continue
                size = int(size)
                # Resets locale to "C" to parse english date properly
                # (non thread-safe code)
                loc = locale.getlocale(locale.LC_TIME)
                locale.setlocale(locale.LC_TIME, 'C')
                date = smbclient.datetime_strptime(date, '%a %b %d %H:%M:%S %Y')
                locale.setlocale(locale.LC_TIME, loc)
                yield (name, modes, size, date)
            #else:
            #    mylog.warning(f"file_pattern error: {filedata}")

    def listdir(self, path, ext='*'):
        """Emulates os.listdir()"""
        #files = super(PatchedSmbClient, self).listdir(path)
        result = [f[0] for f in self.lsdir(path, ext)]
        if not result: # can mean both that the dir is empty or not found
            # disambiguation: verifies if the path doesn't exist. Let the error
            # raised by _getfile propagate in that case.
            self._getfile(path)
        result = list(map(lambda x: x[:-2].strip() if x.endswith('Dn') else x, result))
        result = list(map(lambda x: x[:-3].strip() if x.endswith('DHn') else x, result))
        result = list(map(lambda x: x[:-2].strip() if x.endswith('An') else x, result))
        result = list(map(lambda x: x[:-3].strip() if x.endswith('Atn') else x, result))
        result = list(map(lambda x: x[:-2].strip() if x.endswith('Ac') else x, result))
        return result
        

