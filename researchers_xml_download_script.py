import requests
import pandas as pd
 
print('Download Starting...')
 
base_url = 'https://dblp.org/pid/'

cs_researchers = pd.read_csv('cs_researchers.csv') 

def download(pid,name):
    print("downloading publication profille: "+name)

    r = requests.get(base_url+pid+".xml")

    #I have created researchers_publication folder before hand.
    with open("researchers_publications/"+str(name),'wb') as output_file:
        output_file.write(r.content)
    

cs_researchers.apply(axis=1,
   func = lambda x: download(pid=x.PID,name=x.Name)
)

print('\nDownload Completed!!!')