import sys
import shutil
import os
import importlib
from pymongo import MongoClient
from redlock import Redlock
from datetime import datetime

client = MongoClient()

db_name = sys.argv[1]
traces_path = sys.argv[2]
cache_path = sys.argv[3]
storage_path = sys.argv[4]

if not traces_path[-1] == '/':
    traces_path += '/'

if not cache_path[-1] == '/':
    cache_path += '/'

if not storage_path[-1] == '/':
    storage_path += '/'

os.makedirs(cache_path, exist_ok=True)
os.makedirs(storage_path, exist_ok=True)

db = client[db_name]

dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ])


# Load plugins
plugins = ['time', 'choice', 'var']
analyzers = []

for plugin in plugins:
    mod = importlib.import_module('plugins.' + plugin)
    Analyzer = getattr(mod, 'Analyzer')
    analyzers.append(Analyzer())

gameplays = db.gameplays.find({'modified': True})
for gameplay in gameplays:
    try:
        gameplay_file = '%s' % gameplay['_id']

        lock = False
        while not lock:
            lock = dlm.lock(gameplay_file, 5000)

        try:
            shutil.move(traces_path + gameplay_file, cache_path + gameplay_file)
        except FileNotFoundError as e:
            print(e)
            continue
        finally:
            dlm.unlock(lock)
            db.gameplays.update_one({'_id': gameplay['_id'], 'lastAccessed': gameplay['lastAccessed']},
                                    {'$set': {'modified': False}})

        with open(cache_path + gameplay_file, 'rb') as src:
            with open(storage_path + gameplay_file, 'ab') as dst:
                dst.write(src.read())
        with open(cache_path + gameplay_file, 'r') as f:
            traces = f.read().split('\n')
            collection_name = 'gameplays_results_' + str(gameplay['gameId'])
            if collection_name not in db.collection_names():
                db.create_collection(collection_name)

            gameplays_results = db[collection_name]

            result = gameplays_results.find_one({'gameplayId': gameplay['_id']})
            if not result:
                result = {'gameplayId': gameplay['_id']}
            else:
                del result['_id']
            result['modified'] = True
            result['lastModified'] = datetime.now()
            for trace in traces:
                if not trace:
                    continue
                parts = trace.split(',')
                n = len(parts)
                ms = datetime.fromtimestamp(float(parts[0][:-3]))
                event = parts[1] if n > 1 else None
                target = parts[2] if n > 2 else None
                values = parts[3:]
                for analyzer in analyzers:
                    try:
                        analyzer.trace(result, ms, event, target, values)
                    except Exception as e:
                        print(e)

            gameplays_results.update_one({'gameplayId': gameplay['_id']}, {'$set': result}, True)
        os.remove(cache_path + gameplay_file)
    except Exception as e:
        print(e)

client.close()
