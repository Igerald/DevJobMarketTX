import queue
import urllib
import httplib2
import threading
import pandas as pd
import os,sys,re,time
import matplotlib.pyplot as plt
from urllib.request import Request
from threading import Thread as ted
from time import perf_counter as pctr
from bs4 import BeautifulSoup as soup
from itertools import combinations as cb
from urllib.request import urlopen as urop
from selectors import DefaultSelector,EVENT_WRITE, EVENT_READ
plt.style.use("fivethirtyeight")

q = queue.Queue()
sel = DefaultSelector()
timer = pctr()

def buildjobtitles():
    req = Request("https://www.coderhood.com/19-types-of-developers-explained/", headers={'User-Agent':'Mozilla/5.0'})
    rawdata = urop(req).read()

    rdl = rawdata.split(b"rt-reading-time")[1].split(b"sidebar sidebar-primary widget-area")[0].decode()
    rdl2 = [x for x in re.split("[<>]",rdl) if any(y in x for y in ['Software','Data','Developer'])]
    rdl3 = ''.join(rdl2).split('Software Engineering Job Titles Explained')[1].split("&#8211")[1:18]
    rdl4 = [u.split(' (')[0] if ' (' in u else u for u in [y.strip(";").strip() for y in rdl3]]
    rdl5 = [p.translate(str.maketrans('0123456789','          ')).strip() for p in rdl4[10].split('A big data')[0]]

    jobtitles = rdl9
    return jobtitles

def BuildProgrammingList():
    mdat = pd.read_html("https://www.tiobe.com/tiobe-index/")
    
    plst0 = mdat[0]['Programming Language'].tolist()
    plst1 = mdat[1]['Programming Language'].tolist()
    jobskills = list(sorted(plst1+plst0))
    
    return jobskills

def gettl(Obj):
    tobj = type(Obj)
    print(tobj)
    if tobj == str or tobj == bytes or tobj == bytearray or tobj == list:
        print(len(tobj))

jobtitles = ['Front end Developer', 'Backend Developer', 'Full stack Developer', 'Middle Tier Developer', 'Web Developer', 'Desktop Developer',
           'Mobile Developer', 'Graphics Developer', 'Game Developer', 'Data Scientist', 'Big Data Developer', 'DevOps Developer Developer',
           'Software Development Engineer in Test', 'Embedded Developer', 'High Level Developer', 'Low Level Developer', 'WordPress Developer']

jobskils = ['ABAP', 'Ada', 'Assembly language', 'Bash', 'C', 'C#', 'C++', 'COBOL', 'D', 'Dart', 'Delphi/Object Pascal', 'Erlang', 'Fortran', 'Go',
            'Groovy', 'Hack', 'Haskell', 'Java', 'JavaScript', 'Julia', 'Kotlin', 'LabVIEW', 'Ladder Logic', 'Lisp', 'Logo', 'Lua', 'MATLAB',
            'Objective C', 'PHP', 'PL/I', 'PL/SQL', 'Perl', 'PostScript', 'Prolog', 'Python', 'R', 'RPG', 'Ruby', 'Rust', 'SAS', 'SQL', 'Scala',
            'Scheme', 'Scratch', 'Standard ML', 'Swift', 'Tcl', 'Transact-SQL', 'Visual Basic', 'Visual Basic .NET']

nkey1 = "https://www.indeed.com/jobs?q={Desc}&l="
nikey1 = "https://www.indeed.com/jobs?q={Desc}+{Salary}&l="
niskey1 = "https://www.indeed.com/jobs?q={Desc}+{Salary}&l={City}%2C+{State}"
nskey1 = "https://www.indeed.com/jobs?q={Desc}&l={City}%2C+{State}"

nkey2 = "https://www.indeed.com/jobs?q={Desc}&l="
nikey2 = "https://www.indeed.com/q-{Desc}-${Salary}-jobs.html"
niskey2 = "https://www.indeed.com/q-{Desc}-${Salary}-l-{City},-{State}-jobs.html"
nskey2 = "https://www.indeed.com/jobs?q={Desc}&l={City}%2C+{State}"

mjob = jobtitles+jobskils
salaries = ['50,000','75,000','90,000','100,000','110,000','130,000','160,000','NA']
cities = ['Austin','San Antonio', 'Corpus Christi', 'Dallas','Houston','Fort Worth','NA']
state = ['TX','NA']
plist = [[y.replace('Desc',"").replace('Salary','').replace('City','').replace('State','') for y in x] for x in [[niskey1,niskey2],
                                                                                                                 [nskey1,nskey2],
                                                                                                                 [nikey1,nikey2],
                                                                                                                 [nkey1,nkey2]]]

header = ["Category","Salary","City","State","Jobs","Urls1","Urls2"]

def SpecialReplacements(String):
    return String.strip().replace("+","%2B").replace(" ","+").replace("#","%23").replace("/","%2F")

def databuilder(head,col1,col2,col3,col4,ucol6):
    
    sr = SpecialReplacements
    mset = [[x,y,z] for x in col1 for y in col2 for z in col3]
    mset = [xyz+[col4[0]] if xyz[-1]!="NA" else xyz+[col4[1]] for xyz in mset]
    mset = [ABCD + [ucol6[ABCD.count("NA")][1].format(*[sr(t) for t in ABCD if t!="NA"])] for ABCD in mset]
    mset = [ABCD + [ucol6[ABCD.count("NA")][0].format(*[sr(t).replace(',',"") for t in ABCD[:-1] if t!="NA"])] for ABCD in mset]
    RDF = RetDatFram = ReturnDataFrame = pd.DataFrame(index=range(len(mset)),columns=head)
    RDF[head[0]],RDF[head[1]],RDF[head[2]],RDF[head[3]],RDF[head[5]],RDF[head[6]] = list(zip(*mset))
    
    return RDF

################## Concurrency ##################################################################################################################

class StoppableThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self):
        super(StoppableThread, self).__init__()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()
    

def DataFetcher(url1,url2,row,*args):
    
    global joblist

    count = 1
    if args:
        if type(args) == tuple:
            count = args[0]
        else:
            count = args

    with threading.Lock():
        try:
            try:
                if threading.active_count() > 100: time.sleep(3)
                t = urop(url1)
                rawt = re.split("[<>]",t.read().decode())
                try:
                    trsf =rawt[rawt.index('div id="searchCount"')+1]
                    trsf = int([n.replace(',','') for n in trsf.split(" ") if any(tn in n for tn in '0123456789')][-1])
                except ValueError:
                    trsf = 0
                joblist.append([trsf,row])
                print("length of joblist is " +str(len(joblist)))
                return
            except urllib.error.URLError:
                if threading.active_count() > 100: time.sleep(3)
                t = urop(url2)
                rawt = re.split("[<>]",t.read().decode())
                try:
                    trsf =rawt[rawt.index('div id="searchCount"')+1]
                    trsf = int([n.replace(',','') for n in trsf.split(" ") if any(tn in n for tn in '0123456789')][-1])
                except ValueError:
                    trsf = 0
                joblist.append([trsf,row])
                print("length of joblist is " +str(len(joblist)))
                return
        except (TimeoutError,MemoryError):
            print(threading.active_count())
            time.sleep(3)
            print(count)
            print(url)
            try:
                if count < 20 or pctr < 1000:
                    if count == 10: print("Count as 10 ")
                    return DataFetcher(url,row,count+1)
                else:
                    joblist.append([nan,row])
                    return
            except TypeError:
                print(count)
                exit

        

################## Coroutines ##################################################################################################################

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start



def DataCollector():
    try:
        while True:
            urs = (yield)
            print("Working on Row " + str(urs[-1]))
            t = urop(urs[1])
            trs = re.split("[<>]",t.read().decode())
            try:
                trsf =trs[trs.index('div id="searchCount"')+1]
                trsf = int([n.replace(',','') for n in trsf.split(" ") if any(tn in n for tn in '0123456789')][-1])
            except ValueError:
                trsf = 0
            urs[0].iloc[urs[-1]][4] = trsf
    except GeneratorExit:
        print("")

        

def DataRunner(Collector):
    try:
        while True:
            Initial = (yield)
            print("working ....")
            Collector.send(Initial)
    except GeneratorExit:
            print("Closing Coroutines")

            
########################## Main Run Functions ########################################################################################################
            
def TotalRun(df,carat,RunType):
    global joblist
    joblist=[]
    if RunType == "Cor":
        carat.send(None)
        try:
            for rows in range(df.shape[0]):
                carat.send([df,df.iloc[rows][5],rows])
            carat.close()
        except ValueError:
            pass
        return df
    elif RunType == "Con":
        carats = [carat(target=DataFetcher,args=(urls[0],urls[1],urls[2])) for urls in list(zip(*[df.Urls1.tolist(),df.Urls2.tolist(),list(df.index)]))]
        tlen = len(carats)
        for th in carats:
            if threading.active_count() > 100:
                while threading.active_count() > 50:
                    num =1
            th.start()
        for th in carats:
            th.join()
        df.Jobs = [u[0] for u in sorted(joblist,key=lambda x:x[1])]
        return df
        


def Tester(df,test):
    global md,dc,dr,tr
    ms = df[:20]
    if test == 1:
        ms = tr(ms,dc,"Cor")
    elif test == 2:
        ms = tr(ms,ted,"Con")
    return ms

        
md = MainData = databuilder(header,mjob,salaries,cities,state,plist)
dc = DataCollector()
dr = DataRunner(dc)
tr = TotalRun

ms1 = tr(md,ted,"Con")

nlst = ["Programming Language" if x in jobskils else "Job Title" for x in ms1.Category.tolist()]
ms1["CategoryType"] = nlst

writer = pd.ExcelWriter(os.getcwd()+"\JobsData.xlsx",engine="xlsxwriter")
ms1.to_excel(writer,sheet_name="Job Data")
writer.save()
writer.close()

TotalRuntime = "6.890703078916667 Minutes"

print(str((pctr()-timer)/60.0) + " Minutes")













