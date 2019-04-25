from bs4 import BeautifulSoup as bs
import os
from multiprocessing import Pool, cpu_count
from datetime import datetime
from tqdm import tqdm
from glob import glob

############ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ТУT###############
count_splited = 10

number_of_process = cpu_count() - 1

attr_blacklist = ['ДатаВклМСП', 'ДатаСост', 'ИдДок', 
                    'КатСубМСП', 'ПризНовМСП', 'НаимОрг',
                    'ВерсОКВЭД', 'ВидСубМСП', 'ВерсФорм', 
                    'ИдФайл', 'КолДок', 'ТипИнф']

tag_blacklist = ['СведМН', 'СвОКВЭДДоп', 'СвЛиценз', 'ИдОтпр']
###################################################

def is_non_zero_file(fpath):  
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0

def parseFile(fileName, resFileName):
    
    with open(fileName, encoding='utf-8') as fobj:
        xml = fobj.read()

    soup = bs(xml, features='xml')

    #нахожу все ООО и удаляю
    documents = soup.find_all('Документ', {'ВидСубМСП' : '2'})
    [document.decompose() for document in documents]

    #нахожу все теги и удаляю
    for tag in tag_blacklist:
        sveds = soup.find_all(tag)
        [sved.decompose() for sved in sveds]

    #удаляю все атрибуты из attr_blacklist, а также пустые теги СвОКВЭД
    for tag in soup.find_all(True):
        for attr in attr_blacklist:
            del tag[attr]
        if tag.name == 'СвОКВЭД':
            try:
                 next(tag.children)
            except Exception:
                tag.decompose()
                continue
    
    documents = soup.find_all('Документ')

    if is_non_zero_file(resFileName):
        with open(resFileName, 'r+', encoding='utf-8', errors='ignore') as file:
            xml = file.read()
            soup = bs(xml, features='xml')

            for i in range(len(documents)):
                soup.find('Файл').append(documents[i])

            file.seek(0)
            file.write(str(soup))
    else:
        with open(resFileName, 'w', encoding='utf-8') as file:
            file.write(str(soup))

def parseFiles(args):
    files, number, folderName = args
    resFileName = str(number) + '.xml'
    for file in files:
        parseFile(file, f"{folderName}/{resFileName}")

def parseFolder(folderName):
    files = glob(f"{folderName}/*.xml")

    split_files = []
    n = round(len(files)/count_splited)

    files_pool = []
    for i in range(len(files)):
        if i % n == 0 and i != 0:
            files_pool.append(files[i])
            split_files.append(files_pool)
            files_pool = []
        else:
            files_pool.append(files[i])
            
    if files_pool:
        split_files.append(files_pool)
    
    args = []
    for list_file in split_files:
        args.append((list_file, split_files.index(list_file), f"{folderName}/"))

    with Pool(number_of_process) as p:
        r = list(tqdm(p.imap(parseFiles, args), total=len(args)))

def main():
    number_of_process = cpu_count()
    path = './dataDirs/'

    dirs = os.listdir(path=path)

    try:
        dirs.remove('.DS_Store')
    except Exception:
        pass

    for dir in dirs:
        parseFolder(path+dir)

if __name__ ==  '__main__':
    start = datetime.now()

    main()
    
    end = datetime.now()
    total = end - start
    print(str(total))
    