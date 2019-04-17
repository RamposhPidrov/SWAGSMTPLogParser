import re
import pandas as pd
from tqdm import tqdm
import os
import multiprocessing as mp

class logDF():
    __buffer = None

    def __init__(self, dir=None):
        self.df = pd.DataFrame()
        self.__buffer = []
        if dir:
            self.run_parser(path=dir) 
        else:
            self.run_parser()   

    @property
    def dFrame(self):
        return self.df

    def process_wrapper(self, lineByte, path):
        with open(path, 'r+') as f:
            f.seek(lineByte)
            line = f.readline()
            reg = re.search(r"(\d{2}/\d{2}/\d{2} \d\d:\d\d:\d\d)(?!\s\d)(.+)(?<=\d\s)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\s{2})?(?(4)|\s([A-Z]{4})\s([^\s]{4,}))(\s)?(?(7) ((.)+(?!\d)) | *)\s(.{1,})(?<!\d)(\d{1,})\s(\d{1,})\s((.)+)", line)
            try:
                return {i : reg.group(i) for i in range(1, 14)}
            except:
                pass

    def run_parser(self, path='SMTP.log'):
        filelist = []
        try: 
            filelist = [str.format('{0}/{1}', path, filename) for filename in os.listdir(path)]
        except:
            filelist.append(path)
        for fi in filelist:
            print('Parse file', fi)
            pool = mp.Pool(mp.cpu_count())
            jobs = []
            with open(fi, 'rb+') as f:
                nextLineByte = f.tell()
                for line in f:
                    jobs.append(pool.apply_async(self.process_wrapper, [nextLineByte, path]))
                    nextLineByte = f.tell()   
            self.buffer = filter(lambda a: a != None, [job.get() for job in jobs])
            pool.close()
            self.appendDF()    

    def appendDF(self):
        tmp = pd.DataFrame(self.buffer)
        for i in [4, 7, 8, 9]:
            del tmp[i]
        if (len(self.dFrame) == 0):
            self.df = pd.DataFrame(tmp)
        else:
            self.dFrame.append(tmp, ignore_index=True)
        self.buffer = []


    def nicePrint(self, regmatch):
        print(str.format("Time {0}", regmatch.group(1)))


if __name__ == "__main__":
    #log = logDF('./1')
    log = logDF('./1/SMTP-Activity-181020.log')
    print([i for i in range(0, 10)])
    print(log.df)