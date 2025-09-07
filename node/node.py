import requests

url = "http://127.0.0.1:5000/"

name = input("Name: ")
age = input("Age: ")

payload = {name: "Alice", age: 25}

response = requests.post(url, json=payload)
print(response.json())