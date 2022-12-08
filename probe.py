# probe.py
# Gets mediainfo data from browse copies on the swarm object store
# and stores it into one json file
from subprocess import run
from json import loads, dumps, JSONDecodeError

class MediaInfoException(Exception):
    pass

baseuri='http://<swarmhost>/<source_bucket>/'
with open('mediainfo.json','w') as outfile:
    size=0
    n=0
    outfile.write('[\n')
    with open('objects.txt','r') as infile:
      while obj := infile.readline().strip():
        try:
          p = run(['/usr/bin/mediainfo','-i', baseuri + obj, '--output=JSON'], capture_output=True)
          if p.stderr:
              raise MediaInfoException(p.stderr)
          miout = loads(p.stdout)
          # Some versions of mediainfo return a single object, others a list
          # convert to list if not list 
          if type(miout) is not list:
              miout = [ miout ]
          # when a list is returned, only consider the entries with a valid media section
          info = [ i['media'] for i in miout if i.get('media') is not None ]
          if len(info) != 1:
              raise MediaInfoException('no single mediaInfo found')
          mediainfo = info[0]
          if type(mediainfo.get('track')) is not list:
              raise MediaInfoException('no track info found')
          if not any(t.get('@type') == 'General' for t in mediainfo['track']):
              raise MediaInfoException('no general track info found')
          size += [int(s['FileSize']) for s in mediainfo['track'] if s['@type'] == 'General'][0]
          if n > 0: outfile.write(',\n')
          # replace the ref info with the name of the object
          mediainfo['@ref'] = obj
          outfile.write(dumps(mediainfo))
          n+=1
        except (MediaInfoException, JSONDecodeError) as error:
            print(f'Error: {obj}: {error}')
    outfile.write('\n]\n')
print(n,size)
