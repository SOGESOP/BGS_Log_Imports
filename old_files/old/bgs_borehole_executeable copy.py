# things to do in order of importance 
# add boreholes not downloaded for the misssing/ confidential ones
# add way to check the 'boreholes downlaoded' file against the dataframe to see if anything wasnt downloaded and then indicate why
# add sucess or faliure statements to the logging file
# add in the python script to work through qgis to download the boreholes


import pandas as pd
import requests
import os
import logging
import shutil
# logging is imported in the file setup function as it needs to be done after the old logging file from last run has been deleted

class filters:


    def remove_bad_characters(dataframe):
            # lists the characters that will break the script if they dont go away
            bad_characters=['/']
            # checks through the reference column for words with bad characters
            for row in dataframe['REFERENCE']:
                for char in row:
                    # checks if chaacter in reference is bad and if so replaces it with an underscore
                    if char in bad_characters:
                        row=row.replace(char, '_')
            
    # removes any references from the list that do not have a link to download them
    def remove_missing_url():
        for row, location in zip(df['Record'], df.index):
            if 'Not' in row:
                df.drop(location, inplace=True)
                original_csv=pd.read_csv(str(r'{filepath}\GeoIndexData.txt').format(filepath=os.getcwd()), delimiter='\t')
                # adds am output to the logging file to state which references are removed
                name_missing=original_csv.iloc[location, 1]
                logging.debug(f'{name_missing} has been removed from datatbase as PDF link missing')



    # removes any rows (referemces) that do not have dates
    def remove_missing_date():
        for row, location in zip(df['YEAR_KNOWN'], df.index):
            # this checks if the element can be writen in integer form
            try:
                int(row)
            # this will remove a row from the df if the element in the YEAR_KNOWN row cannot be converted to int (meaning it is string nan)
            except:
                df.drop(location, inplace=True)

    # removes any references that do not have dates within the range provided 
    def date_filter(lower, upper):
        for row, location in zip(df['YEAR_KNOWN'], df.index):  
            try:
                int(row)
                # removes any references that do not fit within the date range provided
                if row not in range(lower, upper):
                    df.drop(location, inplace=True)
            except:
                # notes if there are still references with missing dates, and adds to log file
                logging.error('References missing dates have not been excluded')

    # removes any references that do not have a length (depth) within the given range
    def length_filter(lower, upper):
        for row, location in zip(df['LENGTH'], df.index):
            if row not in range(lower, upper):
                df.drop(location, inplace=True)

    # this will create an excel file of the missing boreholes that were not downloaded
    def not_downloaded():
        # this creates a list of the borehole references that have been downloaded
        boreholes_downloaded=pd.read_excel(f'{os.getcwd()}/boreholes_downloaded.xlsx')
        downloaded_boreholes=list(boreholes_downloaded['REFERENCE'])
        # this creates a list of all the boreholes that exist in the
        original_csv=pd.read_csv(str(r'{filepath}\GeoIndexData.txt').format(filepath=os.getcwd()), delimiter='\t')
        all_boreholes=list(original_csv['REFERENCE'])
        # this adds to logging file how many boreholes were removed
        num_boreholes_removed=len(all_boreholes)-len(downloaded_boreholes)
        logging.info(num_boreholes_removed)
        # this checks which boreholes are missing from the list of downloaded ones.
        for reference in downloaded_boreholes:
            if reference in all_boreholes:
                all_boreholes.remove(reference)
        # creates an excel file of the boreholes that are not downloaded
        missing_boreholes=pd.DataFrame(all_boreholes)
        missing_boreholes.to_excel(f'{os.getcwd()}/boreholes_not_downloaded.xlsx')
     

class name_alteration:
    def reference_date(index_count):
        reference_date=df.iloc[index_count,6]
        return reference_date

    def reference_depth(index_count):
        reference_depth=df.iloc[index_count,5]
        return reference_depth


class main:
    def __init__(self) -> None:
        self.file_setup()
        main.changing_df()
        self.running_downloads()


    # makes a folder in your current working directory called Boreholes
    def file_setup(self):
        # sets up the file path to the boreholes folder for the downloads to go to
        self.pdf_path=f'{os.getcwd()}'+'\Boreholes'
        self.existing_files=os.listdir()
        # adds the borehole log storage folder to the working directory
        # removes any boreholes left over from past runs
        if 'Boreholes' in self.existing_files:
            shutil.rmtree('Boreholes', ignore_errors=True)
        # adds in the boreholes folder
        os.mkdir(self.pdf_path)
        #removes any logging info from previous run
        if 'BGS_import_log.log' in self.existing_files:
            os.remove('BGS_import_log.log')
        
        # sets up the logging file
        logging.basicConfig(level=logging.DEBUG, filename='BGS_import_log.log')
        logging.info(f'{self.existing_files}, {self.pdf_path}')
        # imports the csv to the current working directory
        global df
        df=pd.read_csv(str(r'{filepath}\GeoIndexData.txt').format(filepath=os.getcwd()), delimiter='\t')
        # records the properties of the df into the logging file
        logging.info(df.head(10))
        logging.info(f'{df.size}, {df.shape}')
        logging.info(self.pdf_path)

    # this is used to remove the things from the df that we dont like/want
    def changing_df():
        filters.remove_missing_url()
        # creates temporary csv of the boreholes you are going to download
        df.to_excel(f'{os.getcwd()}/boreholes_downloaded.xlsx')
        filters.remove_bad_characters(df)
        filters.not_downloaded()

        # filters.remove_missing_date()
        # filters.date_filter(1970,1990)


# downloads the pdfs and saves them as the name of the reference column
    def download_pdf_file(self, url: str, pdf_file_name) -> bool:
        # accesses the pdf
        response = requests.get(url)
        if response.status_code==200:
            # finds the folder where the pdf will be saved
            try:
                pdf=open(str(r'{filepath}\{name}'.format(filepath=self.pdf_path, name=pdf_file_name, )), 'wb')
                logging.debug(f'{pdf_file_name} opened.')
                # writes and saves the pdf
                pdf.write(response.content)
                pdf.close()
            except:
                logging.error(f'{pdf_file_name} has not been downloaded')   
            # notifies you the pdf has been downloaded
            logging.info('PDF Downloaded')

    def running_downloads(self):
        # removes extra charatcers from url column
        self.index_count=1
        try:
            for row in df['Record']:
                # adds a count for the row you are looking at so you can use iloc to acces data
                self.index_count+=1
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
                        # records the reference of the borehole being downloaded
                        self.reference_name=df.iloc[self.index_count,1]
                        # names the pdf as you want it, change this to change name
                        self.reference_name=f'{self.reference_name}_{name_alteration.reference_depth(self.index_count)}.pdf'
                        self.download_pdf_file(row,self.reference_name)
                        # limits the script to downloading only 1000 boreholes at a time
                        if self.index_count>1000:
                            exit()
                        break
        # catches any errors in the downloading and creates a log of the error and for which file it occured for
        except Exception:
            logging.exception(f'error has occured while running download for {self.reference_name}')
     
instance=main()