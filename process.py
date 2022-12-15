import pycurl
from subprocess import Popen, PIPE
from multiprocessing import Pool
from io import BytesIO

# Using requests to process media on the fly was abandoned because the requests library does not work well with the swarm object store 
# 1. A 301 received upan a POST is trasnformed into a GET request
# 2. there is no support for expect: 100-continue header.
# 1 can be solved by patching the request library
# 2 cannot easiliy be solved because using chnunked encoding with a generator, the library starts sending the body immediately.
# This results in the unneccesary sending of the body before being redirected to the appropriate node.

# This should be in config file
swarmhost =
sourcebucket =
targetbucket =
domain=

# FFPipeReader
# A read callback returning zero bytes flags the eof and results in curl finalizing the chunked POST operation.
# If the empty buffer is due to an error, then this results in the creation of an empty (or partial) object.
# To avoid this, we wrap the reader callback in a class instance that preservers state information and hence can
# abort the transfer if the empty buffer is due to an error.
class FFPipeReader:
    general_opt = [ '-loglevel' , 'error', '-threads', '1' ]
    uribase = f'http://{swarmhost}/{sourcebucket}/'

    def __init__(self, task):
        cmd = [ '/usr/bin/ffmpeg', '-i', self.uribase + task.name ]
        cmd.extend(self.general_opt)
        cmd.extend(task.transcoding_options)
        cmd.append('pipe:1')
        self.p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        self.error = ''
        self.rc = None

    def read_callback(self, size):
        buffer = self.p.stdout.read(size)
        # Abort the transfer if ffmpeg process terminated with error
        if not buffer:
            self.p.wait(timeout=1)  # we got an empty buffer, so process has exited, no need to wait longly
            self.rc = self.p.returncode
            if self.rc != 0:
               self.error = self.p.stderr.read().decode().strip()
               return pycurl.READFUNC_ABORT
        return buffer

class FFmpegTask:
    transcoding_options = []
    target_ext = ''
    target_mime_type = ''

    def __init__(self, name):
        self.name = name

class ExtractAudio(FFmpegTask):
    transcoding_options = [ '-vn', '-acodec' ,'copy', '-f', 'adts' ]
    target_ext = 'aac'
    target_mime_type = 'audio/aac'

class DropFrames(FFmpegTask):
    transcoding_options = [ '-filter:v', 'setpts=0.1*PTS', '-an', '-f', 'ismv' ]
    target_ext = 'ismv'
    target_mime_type = 'video/mp4'

def postrequest():
       c = pycurl.Curl()
       c.setopt(c.VERBOSE,1)
       c.setopt(c.UPLOAD_BUFFERSIZE,4*1024**3)
       c.setopt(c.POST,1)
       c.setopt(c.FOLLOWLOCATION, 1)
       c.setopt(c.POSTREDIR, pycurl.REDIR_POST_ALL)
       return c

def transcode(task):
    errormsg = ''
    response = BytesIO()
    ff = FFPipeReader(task)
    name = task.name.replace('.mp4', f'.{task.target_ext}')
    try:
       c = postrequest()
       c.setopt(c.URL,f"http://{swarmhost}/{targetbucket}/{name}?domain={domain}")
       c.setopt(c.HTTPHEADER, [ f'Content-Type: {task.target_mime_type}' ])
       c.setopt(c.READFUNCTION, ff.read_callback)
       c.setopt(c.WRITEDATA, response)
       c.perform()
    except pycurl.error as e:
       errormsg = str(e)
    finally: 
       code = c.getinfo(c.HTTP_CODE)
       c.close()
    return (task.name, ff.rc, f'{ff.error}. {errormsg}', code, response.getvalue())


def getmediaobject():
    with open('./browses.txt','r') as mediaobjects:
        while line:=mediaobjects.readline():
            yield (line.strip())

pool=Pool(8)
p = pool.imap_unordered(transcode,( ExtractAudio(o) for o in getmediaobject()))
for i in p:
    print(i)

