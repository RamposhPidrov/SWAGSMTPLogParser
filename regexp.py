import re
import pandas as pd
from tqdm import tqdm
import os
import glob
import multiprocessing as mp
import ipaddress as ip

class Parser():
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
            try: 
                f.seek(lineByte)
                line = f.readline()
                reg = line.split('	')
                #reg = re.search(r"(\d{2}/\d{2}/\d{2} \d\d:\d\d:\d\d)(?!\s\d)(.+)(?<=\d\s)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\s{2})?(?(4)|\s([A-Z]{4})\s([^\s]{4,}))(\s)?(?(7) ((.)+(?!\d)) | *)\s(.{1,})(?<!\d)(\d{1,})\s(\d{1,})\s((.)+)", line)
                try:
                    return reg
                    #return {i : reg.group(i) for i in range(1, 14)}
                except:
                    return None
            except:
                return None

    def run_parser(self, path='SMTP.log'):
        filelist = []
        fil = lambda a: a != None
        try: 
            parsed = glob.glob(str.format('.\\{0}\\*', 'CSV'))
            print(parsed)
            filelist = filter(lambda x: str.format(".\\CSV\\{0}{1}", os.path.basename(x), '.csv') not in parsed,  
                                    glob.glob(str.format('{0}\\*', path)))
        except:
            filelist.append(path)
        for fi in filelist:
            print('Parse file', fi)
            pool = mp.Pool(mp.cpu_count())
            jobs = []
            with open(fi, 'rb+') as f:
                print(os.path.basename(f.name))
                nextLineByte = f.tell()
            
                for line in f:
                    jobs.append(pool.apply_async(self.process_wrapper, [nextLineByte, fi]))
                    nextLineByte = f.tell()   
            self.__buffer = filter(fil, [job.get() for job in jobs[1:-1]])
            pool.close()
            tmp = pd.DataFrame(self.__buffer)
            del tmp[11]
            #for i in [4, 7, 8, 9]:
            #    del tmp[i]
            print(tmp)
            self.df = tmp.copy()
            del tmp
            
            self.__buffer = []
            #self.appendDF()   
            self.saveSCV(os.path.basename(f.name))
            self.df = None
            #self.dFrame = pd.DataFrame()# self.dFrame()


    def appendDF(self):
        tmp = pd.DataFrame(self.__buffer)
        for i in [4, 7, 8, 9]:
            del tmp[i]
        if (len(self.dFrame) == 0):
            self.df = pd.DataFrame(tmp)
        else:
            self.dFrame.append(tmp, ignore_index=True)
        print(self.dFrame)
        self.__buffer = []

    def saveSCV(self, fname):
        self.dFrame.to_csv(str.format('./CSV/{0}.csv', fname), sep='&', encoding='utf-8')

    def nicePrint(self, regmatch):
        print(str.format("Time {0}", regmatch.group(1)))

class Analisis():

    def __init__(self, path='.\\CSV', dir=True):
        self.df = pd.DataFrame()
        self.loadCSV(path)
        self.getCountry()
        

    def loadCSV(self, path, dir=True):
        if dir:
            filelist = glob.glob(str.format("{0}\\*", path))
        else:
            filelist = path
        for i in filelist:
            print(i)
            self.appendDF(pd.read_csv(i, delimiter="&", index_col=0))
            print(self.df)

            
    def appendDF(self, df):
        if (len(self.df) == 0):
            self.df = pd.DataFrame(df)
        else:
            self.df = self.df.append(df, ignore_index=True)

    def getCountry(self):
        cData = pd.read_csv("GeoIPCountryWhois.csv", delimiter=',', names=["ipS", "ipE", "intS", "intE", "Cnt", "Country"])
        del cData['intS']
        del cData['intE']
        cData['ipS'] = cData['ipS'].apply(lambda x: ip.ip_address(x))
        cData['ipE'] = cData['ipE'].apply(lambda x: ip.ip_address(x))
        self.df['3'] = self.df['3'].apply(lambda x: ip.ip_address(x))
        print(cData)
        #print(ip.ip_address(cData['ipS'][1]) < ip.ip_address(self.df['3'][1]))
        #cData.loc(cData['ipS'] < ip.ip_address(self.df['3'][1]))
        


if __name__ == "__main__":
    #Anal = Analisis()
    log = Parser('./1')
    #log = logDF('./1/SMTP-Activity-181020.log')
    #print([i for i in range(0, 10)])
    #print(log.df)