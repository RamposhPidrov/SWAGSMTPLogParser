import re
import pandas as pd
from tqdm import tqdm
import os
import glob
import multiprocessing as mp
import ipaddress as ip
import settings
import seaborn as sns
import matplotlib.pyplot as plt
import sys


class Parser():
    __buffer = None

    def __init__(self, dir=None):
        self.df = pd.DataFrame()
        
        self.cnt = pd.DataFrame()
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
            self.__buffer = filter(fil, [job.get() for job in jobs[1:-2]])
            pool.close()
            tmp = pd.DataFrame(self.__buffer)
            del tmp[11]
            #for i in [4, 7, 8, 9]:
            #    del tmp[i]
            print(tmp)
            self.df = tmp.copy()
            del tmp
            self.getCountry()
            self.__buffer = []
            #self.appendDF()   
            self.saveSCV(os.path.basename(f.name))
            self.df = None
            #self.dFrame = pd.DataFrame()# self.dFrame()

    def binaryIpSearch(self, cData, ipAddr):
        low = 0
        if  len(self.cnt) != 0 and len(self.cnt[self.cnt['ip'] == ipAddr]) != 0:
            #print(type(self.cnt[self.cnt['ip'] == ipAddr]['cnt'].tolist()))
            return self.cnt[self.cnt['ip'] == ipAddr]['cnt'].tolist()[0]
        try:
            ipAddr = ip.ip_address(ipAddr)
        except:
            try: 
                ipAddr = ip.ip_address(str(ipAddr))
            except:
                return None
        high = len(cData) - 1
        found = False
        while low <= high and not found:
            #print(ipAddr)
            mid = (low + high) // 2
            if ip.ip_address(cData['ipS'][mid]) < ipAddr and ip.ip_address(cData['ipE'][mid]) > ipAddr:
                found = True
                self.cnt = self.cnt.append({'ip' : str(ipAddr), 'cnt' : cData['Country'][mid]}, ignore_index=True)
                return cData['Country'][mid]
            else:
                if ipAddr < ip.ip_address(cData['ipS'][mid]):
                    high = mid - 1
                else:
                    low = mid + 1
        #print(ipAddr)
        return None

    def getCountry(self):
        print('cnt')
        cData = pd.read_csv("GeoIPCountryWhois.csv", delimiter=',', names=["ipS", "ipE", "intS", "intE", "Cnt", "Country"])
        #cData['ipS'] = cData['ipS'].apply(lambda x: ip.ip_address(x)) 
        #cData['ipE'] = cData['ipE'].apply(lambda x: ip.ip_address(x)) 
        self.df['Cnt'] = self.df[4].apply(lambda x: self.binaryIpSearch(cData, x))
        #print(ip.ip_address(cData['ipS'][1]) < ip.ip_address(self.df['3'][1]))
        #cData.loc(cData['ipS'] < ip.ip_address(self.df['3'][1]))

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

    def __init__(self, path='.\\CSV', dir=True, spamFilter=True):
        self.df = pd.DataFrame()
        self.bl = pd.DataFrame()
        self.loadCSV(path, spamFilter=spamFilter)
        if spamFilter:
            self.loadBan()
            self.saveBan()
        #self.getCountry()
        print(self.df)
        f, ax = plt.subplots(figsize=(20, 20))
        plot = sns.countplot(y="Cnt", data=self.df, color="c")
        fig = plot.get_figure()
        fig.savefig("output.png")
        self.df['Date'] = self.df['0'].apply(lambda x: x.split(' ')[0])
        plot = sns.countplot(y="Date", data=self.df, color="c")
        fig = plot.get_figure()
        fig.savefig("Date.png")
        #self.Ct.plot(kind='hist', title='Normally distributed random values')
        #plt.show()
        

    def loadCSV(self, path, dir=True, spamFilter=True):
        if dir:
            filelist = glob.glob(str.format("{0}\\*", path))
        else:
            filelist = path
        for i in filelist:
            print(i)
            if spamFilter:
                self.appendDF(self.filterSpam(pd.read_csv(i, delimiter="&", index_col=0), fname=i))
            else:
                self.appendDF((pd.read_csv(i, delimiter="&", index_col=0)))

    def loadBan(self, path='banlist.txt'):
        try:
            self.bl = pd.read_csv(path, delimiter=',', index_col=0)
        except:
            pass

    def saveBan(self, path='banlist.txt'):
        self.bl.to_csv(path, sep=',', encoding='utf-8')

    def appendDF(self, df):
        if (len(self.df) == 0):
            self.df = pd.DataFrame(df)
        else:
            self.df = self.df.append(df, ignore_index=True)

    def filterSpam(self, dataframe, fname='Ban'):
        self.SpamPolicy1(dataframe)
        print(os.path.basename(fname))
        dataframe[dataframe['4'].isin(self.bl['4'])].to_csv(str.format('./SPAM/{0}', os.path.basename(fname)), sep='&', encoding='utf-8')
        return dataframe[~dataframe['4'].isin(self.bl['4'])]

    def SpamPolicy1(self, dataframe):
        le = len(dataframe)
        while len(dataframe) > 0:
            tmp = dataframe[:min(len(dataframe), settings.range)].groupby('4')['0'].count().reset_index()
            #print(tmp)
            dataframe = dataframe.drop(dataframe.index[:min(len(dataframe), settings.range)])
            self.bl = self.bl.append(tmp[tmp['0'] > settings.countforban])
            #self.bl = self.bl.append(pd.DataFrame((tmp[(tmp > settings.countforban) & ~tmp.index[:].isin(self.bl)])), ignore_index=True)
            self.bl = self.bl.groupby('4').sum().reset_index()
            #print(self.bl)
            del tmp


if __name__ == "__main__":
    if sys.argv[1] == None:
        print('Формат ввода: [Директория с логами]')
    else:        
        print('Going to parse log files from:' + sys.argv[1])
        log = Parser(sys.argv[1])
        del log
        Anal = Analisis(path='.\\CSV', spamFilter=True)
        #log = logDF('./1/SMTP-Activity-181020.log')
        #print([i for i in range(0, 10)])
        #print(log.df)
        #an = Analisis()
