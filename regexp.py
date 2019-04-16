import re
import pandas as pd
from tqdm import tqdm

class logDF():

    def __init__(self, path):
        self.df = pd.DataFrame(columns=['Time', 'IP', 'Type', 'TypeA', 'Content' ,'EMail'])
        #print(self.df)
        for i in path:
            #self.loadPandas(i)
            self.parseLogFile(i)            

    @property
    def dFrame(self):
        return self.df

    def parseLogFile(self, path):
        leng = 0
        with open(<pathtofile>) as f:
            leng = len(f.readlines())

        with open(path) as f:
                for line in f:
                    reg = re.search(r"(\d{2}/\d{2}/\d{2} \d\d:\d\d:\d\d)(?!\s\d)(.+)(?<=\d\s)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\s{2})?(?(4)|\s([A-Z]{4})\s([^\s]{4,}))(\s)?(?(7) ((.)+(?!\d)) | *)\s(.{1,})(?<!\d)(\d{1,})\s(\d{1,})\s((.)+)" , line)
                    try:
                        #self.nicePrint(reg)
                        self.sexualMalestedPanda(reg)
                    except:
                        pass


    def nicePrint(self, regmatch):
        print(str.format("Time {0} IP {1:<16} Type {2} Content {3}", regmatch.group(1), regmatch.group(3), regmatch.group(5), regmatch.group(10)))

    def sexualMalestedPanda(self, regmatch):
        self.dFrame.append([regmatch.group(1), regmatch.group(3), regmatch.group(5), regmatch.group(6), regmatch.group(10), regmatch.group(14)])

if __name__ == "__main__":
    log = logDF(['SMTP.log'])
    print(log.dFrame)