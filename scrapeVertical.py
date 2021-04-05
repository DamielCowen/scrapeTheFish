import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta, date

url = 'https://skiwhitefish.com/vertical-tracker/'
path_to_chromeDriver = '/Users/drahcir1/Documents/chromedriver'



import boto3

def pullFromS3(key):
    '''
    
    '''
    bucket = 'scrape-the-fish-bucket'
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, key)


def pushToS3(path, key):
    '''
    path(str) path to file to be pushed
    key(str) filename
    
    pushes file to the s3 scrape-the-fish-bucket
    '''
    bucket = 'scrape-the-fish-bucket' # already created on S3
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(path, bucket, key)
    
    
def getS3Files(bucket='scrape-the-fish-bucket'):
    kwargs = {'Bucket':bucket}
    s3 = boto3.client('s3')
    return s3.list_objects_v2(**kwargs)
    
def getMostRecentDataFile():
    res = getS3Files()
    contents = res['Contents']
    contents.sort(key = lambda obj: obj['LastModified'], reverse = True)
    return contents[0]

    
    
class LeaderBoard:
    
    def __init__(self, driver, output=[]):
        
        self.driver = driver
        if self.driver.current_url != 'https://skiwhitefish.com/vertical-tracker/':
            self.driver.get('https://skiwhitefish.com/vertical-tracker/')
        self.output = output
    
    def __clickButton(self,xPath):
        button = self.driver.find_element_by_xpath(xPath)
        button.click()
        
    def __cleanRow(self,row):
        raw_text = row.get_text()
        raw_text = raw_text.replace('\n','')
        raw_text = raw_text.split('\t')
        return {"name":raw_text[1],
                "vertical":int(raw_text[2].replace(',','')),
                "ranking":int(raw_text[7].replace(',',''))}
    
    def getS3Files(self, bucket='scrape-the-fish-bucket'):
        kwargs = {'Bucket':bucket}
        s3 = boto3.client('s3')
        return s3.list_objects_v2(**kwargs)

    def getMostRecentDataFile(self):
        res = getS3Files()
        contents = res['Contents']
        contents.sort(key = lambda obj: obj['LastModified'], reverse = True)
        return contents[0]


        
    def getLeaderBoardDump(self,category,categoryButton):
        self.__clickButton(categoryButton)
        self.today = date.today()
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source,'lxml')
        season_passes = soup.find("div",{"id":"season_passes"})
        for row in season_passes.find_all("div",{"class":"row"})[2:]:
            cleanData = self.__cleanRow(row)
            self.output.extend([{'date':self.today,
                                 "category":category, 
                                 'name':cleanData['name'],
                                 'vertical':cleanData['vertical'],
                                 "ranking":cleanData['ranking']
                                }])    
    
    def getAllBoards(self):
        self.getLeaderBoardDump("supersenior",'//*[@id="super_senior"]')
        self.getLeaderBoardDump("senior",'//*[@id="senior"]')
        self.getLeaderBoardDump("adult",'//*[@id="adult"]')
        self.getLeaderBoardDump("teen",'//*[@id="teen"]')
        self.getLeaderBoardDump("junior",'//*[@id="junior"]')
        self.getLeaderBoardDump("child",'//*[@id="child"]')
        self.getLeaderBoardDump("college",'//*[@id="college"]')
        return self.output
        
        
        
    
driver = webdriver.Chrome(executable_path=path_to_chromeDriver)
driver.implicitly_wait(5)
Board = LeaderBoard(driver)
res = Board.getAllBoards()
res.sort(key = lambda skier: skier['ranking'])
driver.close()

df = pd.DataFrame(res)
df.index.name = 'index'


yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
today = datetime.today().strftime('%Y%m%d')

lastFile = getMostRecentDataFile()
lastFile = lastFile['Key']
pullFromS3(lastFile)
df_yesterday = pd.read_csv(lastFile, index_col = 'index')


df = df.append(df_yesterday)

df_filename = 'data{}.csv'.format(today)
backup_filename = 'backup{}.csv'.format(today)

df.to_csv(df_filename)
df.to_csv(backup_filename)

pushToS3('HelloWorld.txt','HelloWorld')
#pushToS3(df_filename,df_filename+'test')
#pushToS3(backup_filename,df_filename)