import sys
from pymongo import MongoClient

client = MongoClient()

db_name = sys.argv[1]
game_ids = sys.argv[2].split(';')
scores = sys.argv[3].split(';')

db = client[db_name]
i = 0
for game_id in game_ids:
    collection_name = 'gameplays_results_' + game_id

    if collection_name not in db.collection_names():
        i += 1
        continue

    gameplays_results = db[collection_name]

    collection_name = 'gameplays_assessments_' + game_id
    if collection_name not in db.collection_names():
        db.create_collection(collection_name)
    gameplays_assessments = db[collection_name]

    collection_name = 'players_assessments_' + game_id
    if collection_name not in db.collection_names():
        db.create_collection(collection_name)
    players_assessments = db[collection_name]

    gameplays = db.gameplays
    players = []
    for result in gameplays_results.find({'modified': True}):
        gameplay = gameplays.find_one({'_id': result['gameplayId']})
        if not gameplay:
            continue

        player_id = gameplay['playerId']
        players.append(player_id)
        score = 0
        if 'vars' in result:
            score = result['vars'][scores[i]] if scores[i] in result['vars'] else 0

        gameplays_assessments.update_one({'playerId': player_id, 'gameplayId': result['gameplayId']},
                                         {'$set': {'score': score}}, True)
        gameplays_results.update_one({'_id': result['_id'], 'lastModified': result['lastModified']},
                                     {'$set': {'modified': False}})

    for player in players:
        assessments = gameplays_assessments.find({'playerId': player})
        score = 0
        for assessment in assessments:
            score = max(assessment['score'], score)
        players_assessments.update_one({'playerId': player}, {'$set': {'score': score}}, True)

    i += 1

client.close()
