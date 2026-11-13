"""现有执行路径。"""
import hashlib
import threading

class ObjectReader:
    def __init__(self,objects):
        self.objects={(o['name'],o['version']):o['text'].encode('utf-8') for o in objects}
        self.calls=[]; self.lock=threading.Lock()
    def read(self,name,version,start,length):
        data=self.objects[(name,version)]
        if type(start) is not int or type(length) is not int or min(start,length)<0 or start+length>len(data):raise ValueError('读取范围越界')
        with self.lock:self.calls.append((name,version,start,length))
        return data[start:start+length]

class GateReader(ObjectReader):
    def __init__(self,objects):
        super().__init__(objects); self.started=threading.Event(); self.release=threading.Event(); self.error=None
    def read(self,*args):
        self.started.set()
        if not self.release.wait(timeout=10):raise TimeoutError('读取门未放行')
        if self.error is not None:raise self.error
        return super().read(*args)

def read_one(reader,request):
    result=reader.read(request['object'],request['version'],request['start'],request['length'])
    if len(result)!=request['length']:raise ValueError('对象短读')
    if request.get('sha256') and hashlib.sha256(result).hexdigest()!=request['sha256']:raise ValueError('页校验失败')
    return result

def execute(data):
    reader=ObjectReader(data['objects'])
    return [{'query':r['query'],'bytes_hex':read_one(reader,r).hex()} for r in data['requests']]
