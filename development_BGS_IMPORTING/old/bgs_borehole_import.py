import pandas as pd
import requests
import logging
logging.basicConfig(level=logging.DEBUG, filename='BGS_import.log')



# imports the csv
df=pd.read_csv(r'C:\Users\OGONAHT\OneDrive - Jacobs\NEAR\Python importing\GeoIndexData.txt', delimiter='\t')
print(df.head(10))
print(df.size)

# creates datasets for just the colunns to be iterated through
urls=df['Record']
references=df['REFERENCE']        

# downloads the pdfs and saves them as the name of the reference column
def download_pdf_file(url: str, pdf_file_name) -> bool:
    # accesses the pdf
    response = requests.get(url)
    if response.status_code==200:
        # finds the folder where the pdf will be saved
        pdf=open(str(r'C:\Users\OGONAHT\OneDrive - Jacobs\NEAR\Python importing\PDF\{dates}\{name}'.format(name=pdf_file_name, dates='Including_Dates')), 'wb')
        # writes and saves the pdf
        pdf.write(response.content)
        pdf.close()
        # notifies you the pdf has been downloaded
        logging.info('PDF Downloaded')

# removes extra charatcers from url column
index_count=1
for row in urls:
    # adds a count for the row you are looking at so you can use iloc to acces data
    index_count+=1
    # saves a copy of the data before it is modified
    b=row
    # removes the excess characters from before the url
    row=row[9:]
    #removes the excess characters from after the url
    char_count=0
    for x in row:
        char_count+=1
        if x=="'":
            row=row[:char_count-1]
            # records the reference, date, and depth of the boreholes
            reference_name=df.iloc[index_count,1]
            reference_date=df.iloc[index_count,6]
            reference_depth=df.iloc[index_count,5]
            # names the pdf as you want it
            reference_name=f'{reference_name}_{reference_date}.pdf'
            download_pdf_file(row,reference_name)
            # stops the script after x boreholes
            # if index_count>10:
            #     exit()
            break
    