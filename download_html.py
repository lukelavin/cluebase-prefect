import requests

r = requests.get('https://j-archive.com/listseasons.php')

with open('downloaded.html', 'w') as f:
    f.write(r.text)