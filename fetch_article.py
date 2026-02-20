import urllib.request

url = "https://medium.com/@ishaan.rawat611/data-engineering-project-using-aws-lambda-glue-athena-and-quicksight-5b9241c8510a"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

try:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        content = response.read().decode('utf-8')
        with open("article_content.html", "w", encoding="utf-8") as f:
            f.write(content)
    print("Successfully fetched article.")
except Exception as e:
    print(f"Error: {e}")
