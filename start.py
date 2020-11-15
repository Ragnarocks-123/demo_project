'''
Индивидуальная проектная работа.Школа Data Engineer (Поток 4, 2020). 
Кириллов Даниил Владимирович (Ragnarocks)
Разработать ETL процесс, получающий ежедневную выгрузку данных (за 3 дня), загружающий ее в хранилище данных и ежедневно строящий отчет.
'''
import os
import glob
import subprocess
import re

os.system('cls' if os.name == 'nt' else 'clear')
os.chdir("./")

files_1_1 = glob.glob('passports_blacklist_????????.xlsx')
files_2_2 = glob.glob('transactions_????????.xlsx')

print('\nНайдены следующие файлы для загрузки:\n')

print('Файл выгрузки паспортов из «черного списка» типа passport_blacklist_DDMMYYYY.xlsx:')
if len(files_1_1) == 0:
	print('файлы не найдены')
else:
	for files_1 in glob.glob("passports_blacklist_????????.xlsx"):
		print(files_1)

print('\nФайл выгрузки транзакций типа transactions_DDMMYYYY.xlsx:')
if len(files_2_2) == 0:
	print('файлы не найдены')
else:
	for files_2 in glob.glob("transactions_????????.xlsx"):
		print(files_2)

if len(files_1_1) == 0 or len(files_2_2) == 0: 
	print('\nВ директории отсутствуют необходимые для загрузки файлы, процедура загрузки прервана!')
	os.system('pause')
	raise SystemExit

if len(files_1_1) > 1 or len(files_2_2) > 1: 
	print('\nВ директории найдено более одного файла типа passport_blacklist_DDMMYYYY.xlsx и/или transactions_DDMMYYYY.xlsx')
	print('Cкоректируйте количество файлов, процедура загрузки прервана!')
	os.system('pause')
	raise SystemExit

if re.findall(r'\d+', files_1_1[0]) != re.findall(r'\d+', files_2_2[0]): 
	print('\nВНИМАНИТЕ! Не совпадают даты в названии загружаемых файлов passport_blacklist_DDMMYYYY.xlsx и transactions_DDMMYYYY.xlsx.')
	print('Проверьте загружаемые файлы или откорректируйте даты в название файлов')
	os.system('pause')
	raise SystemExit 
	
else:
	print('\nНачинаю процедуру загрузки файлов:'' ' + files_1 + ', ' + files_2 + '\n')

p = subprocess.Popen("python main.py" + ' ' + files_1 + ' ' + files_2, shell=True, bufsize=2048)
p.wait()
if p.returncode == 0:
    pass 
    os.replace(files_1, files_1 + '.backup')
    os.replace(files_2, files_2 + '.backup')

print('\nПроцедура выполнена! Файлы ' + files_1 + ', ' + files_2 + ' переименованы в файлы с расширением .backup.')
