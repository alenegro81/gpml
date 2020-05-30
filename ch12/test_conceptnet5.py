import requests
obj = requests.get('http://api.conceptnet.io/c/en/marie_curie').json()
print(obj['edges'][0]['rel']['label'] + ": " + obj['edges'][0]['end']['label'])