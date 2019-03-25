#read txt file deliminted by = and export to csv
import pandas as pd 

with open('Reveal_Report.txt') as f:
    first_line = f.readline()
    first_line = first_line.replace('"', '')
    first_line = first_line.split(",")

finalList = pd.read_csv('Reveal_Report.txt', names=first_line, skiprows=1, sep='=', encoding="ISO-8859-1")
print(finalList)

finalList.to_csv("Needles.csv",index=False)


