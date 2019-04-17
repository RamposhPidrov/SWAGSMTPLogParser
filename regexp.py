import re
import pandas as pd
from tqdm import tqdm

import os
import multiprocessing as mp

class logDF():
    buffer = None

    def __init__(self, path):
        self.df = pd.DataFrame()
        
        self.buffer = []
        #self.run_multiprocess(path)
        self.run_mp(path)
        self.appendDF()
        #print(self.df)
        #for i in path:
            #self.loadPandas(i)
        #    self.parseLogFile(i)            

    @property
    def dFrame(self):
        return self.df

    def process_wrapper(self, lineByte, path):
        with open(path, 'r+') as f:
            f.seek(lineByte)
            line = f.readline()
            
            reg = re.search(r"(\d{2}/\d{2}/\d{2} \d\d:\d\d:\d\d)(?!\s\d)(.+)(?<=\d\s)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\s{2})?(?(4)|\s([A-Z]{4})\s([^\s]{4,}))(\s)?(?(7) ((.)+(?!\d)) | *)\s(.{1,})(?<!\d)(\d{1,})\s(\d{1,})\s((.)+)" , line)
            try:
                return {i : reg.group(i) for i in range(1, 14)}
            except:
                pass

    def run_mp(self, path):
        pool = mp.Pool(mp.cpu_count())
        jobs = []
        with open(path, 'rb+') as f:
            nextLineByte = f.tell()
            for line in f:
                #print(nextLineByte)
                jobs.append(pool.apply_async(self.process_wrapper, [nextLineByte, path]))
                
                #print(type(jobs[0]))
                nextLineByte = f.tell()
        
        self.buffer = [job.get() for job in jobs]
        pool.close()
        #print(self.buffer)
        

    def appendDF(self):
        tmp = pd.DataFrame(self.buffer)
        for i in [4, 7, 8, 9]:
            del tmp[i]
        if (len(self.dFrame) == 0):
            self.df = pd.DataFrame(tmp)
        else:
            
            self.dFrame.append(tmp, ignore_index=True)

        self.buffer = []

    def run_multiprocess(self, path):
        tasks = []

        for filename in os.listdir(path):
            tasks.append(str.format('./1/{0}',filename))
            print('Create task', filename)

        pool = mp.Pool(2)
        result = all(list(pool.imap_unordered(self.parseLogFile, tasks)))

    def nicePrint(self, regmatch):
        print(str.format("Time {0}", regmatch.group(1)))
        #print(str.format("Time {0} IP {1:<16} Type {2} Content {3}", regmatch.group(1), regmatch.group(3), regmatch.group(5), regmatch.group(10)))


if __name__ == "__main__":
    #log = logDF('./1')
    log = logDF('SMTP.log')
    print([i for i in range(0, 10)])
    print(log.df)