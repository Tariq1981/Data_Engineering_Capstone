import requests
from bs4 import BeautifulSoup

url="https://www.dhs.gov/immigration-statistics/nonimmigrant/NonimmigrantCOA"
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")
tables=soup.find_all(name="table")
#print(tables)
with open("./immigration/Temp_Non-Immigrant.csv","w") as file:
    file.write("Code|Description\n")
    for table in tables:
        trs=table.find_all("tr")
        for row in trs:
            code = row.find("th")
            desc = row.find("td")
            print("Code: ",code.text,"| Desc: ",desc.text)
            file.write(code.text+"|"+desc.text+"\n")
