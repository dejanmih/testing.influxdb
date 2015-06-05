import tempfile
import socket
import os
import signal
from shutil import rmtree
from time import sleep
from datetime import datetime

from clom import clom
import requests
import ConfigParser


__all__ = ['InfluxDBServer']


def _unused_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    _, port = sock.getsockname()
    sock.close()
    return port

class FakeGlobal(object):
    def __init__(self, fp):
        self.fp = fp
        self.section = '[global]\n'

    def readline(self):
        if self.section:
            try: 
                return self.section
            finally: 
                self.section = None
        else: 
            return self.fp.readline()


class InfluxDBServer(object):
    def __init__(self, root=None, cmd=None, cfg_file=None, port=None, admin='root', password=None, foreground=True):
        """
        root:  Root directory of InfluxDB service. If set, it will not
               be removed. If not set, data and log files will be removed.
        cmd: The `influxdb` command to run if `influxdb` is not in $PATH.
        Usage of this class:
            with InfluxDBServer(root="/opt/influxdb/shared") as ts:
                ts = YourInfluxDBClient(ts.uri())
            # influxdb terminated here when context exits
        """
        if cmd is None:
            cmd = str(clom.which('influxdb').shell())
        self._cmd = cmd

        self._foreground = foreground

        self._use_tmp_dir = False
        self._root = root
        self._cfg_file = cfg_file
        self._host = None
        self._port = port
        self._admin = admin
        self._password = password
        self._ts_pid = None
        self._owner_pid = os.getpid()

    def __del__(self):
        self.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def uri(self):
        """
        Returns the influxdb uri for the testing instance.
        """
        args = [self._admin, self._password, self._host, self._port]
        if not all(args):
            raise ValueError('URI with admin info could not be formed from %s' % json.dumps(args))
        return "http://{0._admin}:{0._password}@{0._host}:{0._port}".format(self)

    def _update_config_file(self):
        cfg_path = '{cfg_dir}/{cfg_file}'.format(cfg_dir=self._root, cfg_file=self._cfg_file)

        if not cfg_path or not self._port:
            return

        # ConfigParser doesn't like leading spaces so we need to strip them off
        fh = open(cfg_path, "r")
        stripped_lines = [l.lstrip() for l in fh.readlines() if l.lstrip()]
        fh.close()
        fh = open(cfg_path, "w")
        fh.writelines(stripped_lines)
        fh.close()

        config = ConfigParser.SafeConfigParser()
        with open(cfg_path, 'r') as cfile:

            # ConfigParser doesn't like stuff out of sections, so fake global section
            config.readfp(FakeGlobal(cfile))

            # Set unused port for InfluxDB to bind to for API listener
            config.set('api', 'port', str(self._port))
    
            # Set logging path
            config.set('logging', 'file', '"' + self._root + '/data/log.txt"')
    
            # Set storage path
            config.set('storage', 'dir', '"' + self._root + '/data/db"')

        # Save changed parameters to the configuration
        with open(cfg_path, 'wb') as cfile:
            config.write(cfile)

        # Remove fake global section line
        with open(cfg_path,"r+") as cfile:
            d = cfile.readlines()
            cfile.seek(0)
            for i in d:
                if i != '[global]\n':
                    
                    # ConfigParser got confused by this section - fixing it
                    if i == '[[input_plugins.udp_servers]\n':
                        i = '[[input_plugins.udp_servers]]\n'

                    cfile.write(i)
                    cfile.truncate()

    def _configure(self, default_dir='/opt/influxdb/shared', default_cfg_file='config.toml'):
        """
        Configure this instance of influxdb before trying to start the
        server.
        This method exists mainly for unit test hooks so that we can prepare
        to start the influxdb server without actually making it start up,
        which takes a relatively long time.
        """
        # setup root dir
        if not self._root:
            self._use_tmp_dir = True
            self._root = default_dir

        if not self._cfg_file:
            self._cfg_file = default_cfg_file

        if not self._port:
            self._port = _unused_port()

        self._update_config_file() 

        self._host = '127.0.0.1'
  
        if self._password:
            os.environ['INFLUXDB_INIT_PWD'] = self._password

    def start(self):
        """
        Start the test influxdb instance.
        """
        if self._ts_pid:
            # already started
            return

        self._configure()

        pid = os.fork()
        if pid == 0:

            try:
                os.execl(
                    self._cmd,
                    self._cmd,
                    '-config',
                    self._root + '/' + self._cfg_file,
                )
            except Exception:
                raise RuntimeError("Could not start InfluxDB.")
        else:
            while True:
                try:
                    params = { 'u': self._admin, 'p': self._password }
                    result = requests.get(self.uri()+'/db', params=params)
                    if result.status_code == 200:
                        break
                except requests.exceptions.ConnectionError:
                    # service not up yet, ignore.
                    pass

                if os.waitpid(pid, os.WNOHANG)[0] != 0:
                    raise RuntimeError("Failed to start InfluxDB.")

                sleep(0.5)
            self._ts_pid = pid

    def stop(self):
        """
        Stop the test InfluxDB instance.
        """
        if self._owner_pid == os.getpid() and self._ts_pid:
            self._terminate()
            self._cleanup()

    def _terminate(self):
        os.kill(self._ts_pid, signal.SIGTERM)
        killed_at = datetime.now()

        try:
            while (os.waitpid(self._ts_pid, os.WNOHANG)):
                if (datetime.now() - killed_at).seconds > 10.0:
                    os.kill(self._ts_pid, signal.SIGKILL)
                    raise RuntimeError("Unable to cleanly stop InfluxDB.")
                sleep(0.1)
        except:
            # Child process already terminated.
            pass

        self._bind_host = None
        self._bind_port = None
        self._ts_pid = None

    def _cleanup(self):
        if self._use_tmp_dir and os.path.exists(self._root):
            rmtree(self._root + '/data', ignore_errors=True)
