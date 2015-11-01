import sys
from pymongo import MongoClient

client = MongoClient()

db_name = sys.argv[1]
game_id = sys.argv[2]

db = client[db_name]

collection_name = 'gameplays_results_' + game_id

if collection_name not in db.collection_names():
    exit()

gameplays_results = db[collection_name]

collection_name = 'gameplays_assessments_' + game_id
if collection_name not in db.collection_names():
    db.create_collection(collection_name)
gameplays_assessments = db[collection_name]

collection_name = 'players_assessments_' + game_id
if collection_name not in db.collection_names():
    db.create_collection(collection_name)
players_assessments = db[collection_name]

correct = {
    'Pregunta': 'Respuesta'
}

gameplays = db.gameplays
players = []
for result in gameplays_results.find({'modified': True}):
    gameplay = gameplays.find_one({'_id': result['gameplayId']})
    if not gameplay:
        continue

    player_id = gameplay['playerId']
    players.append(player_id)
    score = 0
    if 'choices' in result:
        choices = result['choices']
        for key in correct:
            value = correct[key]
            score += 0 if key not in choices or value not in choices[key] else choices[key][value]

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

client.close()
