import json
import requests

def lambda_handler(event, context):
    urls = ["https://api.chucknorris.io/jokes/random",
            "http://api.icndb.com/jokes/random"]

    final_data = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36'}

    for url in urls:
        data = requests.get(url, headers=headers).json()
        final_data.append((data))

    with open('output.json', 'w') as f:
        for fd in final_data:
            json.dump(fd, f)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda How are you Man!')
    }
